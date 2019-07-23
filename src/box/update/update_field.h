#ifndef TARANTOOL_BOX_TUPLE_UPDATE_FIELD_H
#define TARANTOOL_BOX_TUPLE_UPDATE_FIELD_H
/*
 * Copyright 2010-2019, Tarantool AUTHORS, please see AUTHORS file.
 *
 * Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following
 * conditions are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer in the documentation and/or other materials
 *    provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY <COPYRIGHT HOLDER> ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED
 * TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL
 * <COPYRIGHT HOLDER> OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
 * BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 */
#include "trivia/util.h"
#include "tt_static.h"
#include <stdint.h>
#include "json/json.h"
#include "bit/int96.h"
#include "mp_decimal.h"

/**
 * This file is a link between all the update operations for all
 * the field types. It functions like a router, when an update
 * operation is being parsed step by step, going down the update
 * tree. For example, when an update operation goes through an
 * array, a map, another array, and ends with a scalar operation,
 * at the end of each step the operation goes to the next one via
 * functions of this file: update_array.c -> update_field.c ->
 * update_map.c -> update_field.c -> update_array.c -> ... . The
 * functions, doing the routing, are do_op_<operation_type>(), see
 * them below.
 */

struct rope;
struct update_field;
struct update_op;
struct tuple_dictionary;

/* {{{ update_op */

/** Argument of SET (and INSERT) operation. */
struct op_set_arg {
	uint32_t length;
	const char *value;
};

/** Argument of DELETE operation. */
struct op_del_arg {
	uint32_t count;
};

/**
 * MsgPack format code of an arithmetic argument or result.
 * MsgPack codes are not used to simplify type calculation.
 */
enum arith_type {
	AT_DECIMAL = 0, /* MP_EXT + MP_DECIMAL */
	AT_DOUBLE = 1, /* MP_DOUBLE */
	AT_FLOAT = 2, /* MP_FLOAT */
	AT_INT = 3 /* MP_INT/MP_UINT */
};

/**
 * Argument (left and right) and result of ADD, SUBTRACT.
 *
 * To perform an arithmetic operation, update first loads left
 * and right arguments into corresponding value objects, then
 * performs arithmetics on types of arguments, thus calculating
 * the type of the result, and then performs the requested
 * operation according to the calculated type rules.
 *
 * The rules are as follows:
 * - when one of the argument types is double, the result is
 *   double;
 * - when one of the argument types is float, the result is float;
 * - for integer arguments, the result type code depends on the
 *   range in which falls the result of the operation. If the
 *   result is in negative range, it's MP_INT, otherwise it's
 *   MP_UINT. If the result is out of bounds of (-2^63, 2^64), an
 *   exception is raised for overflow.
 */
struct op_arith_arg {
	enum arith_type type;
	union {
		double dbl;
		float flt;
		struct int96_num int96;
		decimal_t dec;
	};
};

/** Argument of AND, XOR, OR operations. */
struct op_bit_arg {
	uint64_t val;
};

/** Argument of SPLICE. */
struct op_splice_arg {
	/** Splice position. */
	int32_t offset;
	/** Byte count to delete. */
	int32_t cut_length;
	/** New content. */
	const char *paste;
	/** New content length. */
	uint32_t paste_length;

	/** Offset of the tail in the old field. */
	int32_t tail_offset;
	/** Size of the tail. */
	int32_t tail_length;
};

/** Update operation argument. */
union update_op_arg {
	struct op_set_arg set;
	struct op_del_arg del;
	struct op_arith_arg arith;
	struct op_bit_arg bit;
	struct op_splice_arg splice;
};

typedef int
(*update_op_do_f)(struct update_op *op, struct update_field *field);

typedef int
(*update_op_read_arg_f)(struct update_op *op, const char **expr,
			int index_base);

typedef void
(*update_op_store_f)(struct update_op *op, const char *in, char *out);

/**
 * A set of functions and properties to initialize, do and store
 * an operation.
 */
struct update_op_meta {
	/**
	 * Virtual function to read the arguments of the
	 * operation.
	 */
	update_op_read_arg_f read_arg_cb;
	/** Virtual function to execute the operation. */
	update_op_do_f do_cb;
	/**
	 * Virtual function to store a result of the operation.
	 */
	update_op_store_f store_cb;
	/** Argument count. */
	uint32_t arg_count;
};

/** A single UPDATE operation. */
struct update_op {
	/** Operation meta depending on the op type. */
	const struct update_op_meta *meta;
	/** Operation arguments. */
	union update_op_arg arg;
	/**
	 * Current level token. END means that it is invalid and
	 * a next token should be extracted from the lexer.
	 */
	enum json_token_type token_type;
	union {
		struct {
			const char *key;
			uint32_t key_len;
		};
		int32_t field_no;
	};
	/** Size of a new field after it is updated. */
	uint32_t new_field_len;
	/** Opcode symbol: = + - / ... */
	char opcode;
	/**
	 * Operation target path and its lexer in one. This lexer
	 * is used when the operation is applied down through the
	 * update tree.
	 */
	struct json_lexer lexer;
};

/**
 * Extract a next token from the operation path lexer. The result
 * is used to decide to which child of a current map/array the
 * operation should be forwarded. It is not just a synonym to
 * json_lexer_next_token, because fills some fields of @a op,
 * and should be used only to chose a next child inside a current
 * map/array.
 */
int
update_op_consume_token(struct update_op *op);

/**
 * Decode an update operation from MessagePack.
 * @param[out] op Update operation.
 * @param index_base Field numbers base: 0 or 1.
 * @param dict Dictionary to lookup field number by a name.
 * @param expr MessagePack.
 *
 * @retval 0 Success.
 * @retval -1 Client error.
 */
int
update_op_decode(struct update_op *op, int index_base,
		 struct tuple_dictionary *dict, const char **expr);

/**
 * Check if the operation should be applied on the current path
 * node.
 */
static inline bool
update_op_is_term(const struct update_op *op)
{
	return json_lexer_is_eof(&op->lexer);
}

/* }}} update_op */

/* {{{ update_field */

/** Types of field update. */
enum update_type {
	/**
	 * Field is not updated. Just save it as is. It is used,
	 * for example, when a rope is split in two parts: not
	 * changed left range of fields, and a right range with
	 * its first field changed. In this case the left range is
	 * NOP.
	 */
	UPDATE_NOP,
	/**
	 * Field is a scalar value, updated via set, arith, bit,
	 * splice, or any other scalar-applicable operation.
	 */
	UPDATE_SCALAR,
	/**
	 * Field is an updated array. Check the rope for updates
	 * of individual fields.
	 */
	UPDATE_ARRAY,
	/**
	 * Field of this type stores such update, that has
	 * non-empty JSON path non-intersected with any another
	 * update. In such optimized case it is possible to do not
	 * allocate neither fields nor ops nor anything for path
	 * nodes. And this is the most common case.
	 */
	UPDATE_BAR,
	/**
	 * Field with a subtree of updates having the same prefix,
	 * stored here explicitly. New updates with the same
	 * prefix just follow it without decoding of JSON nor
	 * MessagePack. It can be quite helpful when an update
	 * works with the same internal object via several
	 * operations like this:
	 *
	 *    [1][2].a.b.c[3].key1 = 20
	 *    [1][2].a.b.c[3].key2 = 30
	 *    [1][2].a.b.c[3].key3 = true
	 *
	 * Here [1][2].a.b.c[3] is stored only once as a route
	 * with a subtree 'key1 = 20', 'key2 = 30', 'key3 = true'.
	 */
	UPDATE_ROUTE,
};

/**
 * Generic structure describing update of a field. It can be
 * tuple field, field of a tuple field, or any another tuple
 * internal object: map, array, scalar, or unchanged field of any
 * type without op. This is a node of the whole update tree.
 *
 * If the field is array or map, it contains more children fields
 * inside corresponding sub-structures.
 */
struct update_field {
	/**
	 * Type of this field's update. Union below depends on it.
	 */
	enum update_type type;
	/** Field data to update. */
	const char *data;
	uint32_t size;
	union {
		/**
		 * This update is terminal. It does a scalar
		 * operation and has no children fields.
		 */
		struct {
			struct update_op *op;
		} scalar;
		/**
		 * This update changes an array. Children fields
		 * are stored in rope nodes.
		 */
		struct {
			struct rope *rope;
		} array;
		/**
		 * Bar update - by JSON path non-intersected with
		 * any another update.
		 */
		struct {
			/** Bar update is a single operation. */
			struct update_op *op;
			/**
			 * Always has a non-empty head path
			 * leading inside this field's data.
			 */
			const char *path;
			int path_len;
			/**
			 * For insertion/deletion to change parent
			 * header.
			 */
			const char *parent;
			union {
				/**
				 * For scalar op; insertion into
				 * array; deletion. This is the
				 * point to delete, change or
				 * insert after.
				 */
				struct {
					const char *point;
					uint32_t point_size;
				};
				/*
				 * For insertion into map. New
				 * key. On insertion into a map
				 * there is no strict order as in
				 * array and no point. The field
				 * is inserted just right after
				 * the parent header.
				 */
				struct {
					const char *new_key;
					uint32_t new_key_len;
				};
			};
		} bar;
		/** Route update - path to an update subtree. */
		struct {
			/**
			 * Common prefix of all updates in the
			 * subtree.
			 */
			const char *path;
			int path_len;
			/** Update subtree. */
			struct update_field *next_hop;
		} route;
	};
};

/**
 * Size of the updated field, including all children recursively.
 */
uint32_t
update_field_sizeof(struct update_field *field);

/** Save the updated field, including all children recursively. */
uint32_t
update_field_store(struct update_field *field, char *out, char *out_end);

/**
 * Generate declarations for a concrete field type: array, bar
 * etc. Each complex type has basic operations of the same
 * signature: insert, set, delete, arith, bit, splice.
 *
 * These operations can call each other going down a JSON path.
 */
#define OP_DECL_GENERIC(type)							\
int										\
do_op_##type##_insert(struct update_op *op, struct update_field *field);	\
										\
int										\
do_op_##type##_set(struct update_op *op, struct update_field *field);		\
										\
int										\
do_op_##type##_delete(struct update_op *op, struct update_field *field);	\
										\
int										\
do_op_##type##_arith(struct update_op *op, struct update_field *field);		\
										\
int										\
do_op_##type##_bit(struct update_op *op, struct update_field *field);		\
										\
int										\
do_op_##type##_splice(struct update_op *op, struct update_field *field);	\
										\
uint32_t									\
update_##type##_sizeof(struct update_field *field);				\
										\
uint32_t									\
update_##type##_store(struct update_field *field, char *out, char *out_end);

/* }}} update_field */

/* {{{ update_field.array */

/**
 * Initialize @a field as an array to update.
 * @param[out] field Field to initialize.
 * @param header Header of the MessagePack array @a data.
 * @param data MessagePack data of the array to update.
 * @param data_end End of @a data.
 * @param field_count Field count in @data.
 *
 * @retval  0 Success.
 * @retval -1 Error.
 */
int
update_array_create(struct update_field *field, const char *header,
		    const char *data, const char *data_end,
		    uint32_t field_count);

/**
 * The same as mere create, but the array is created with a first
 * child already. It allows to make it more effective, because
 * under the hood a rope is created not as a one huge range which
 * is then spit in parts, but as two rope nodes from the
 * beginning. On the summary: -1 rope node split, -1 decoding of
 * fields to @a field_no.
 *
 * The function is used during branching, where there was an
 * existing update, but another one came with the same prefix, and
 * a different suffix.
 *
 * @param[out] field Field to initialize.
 * @param child A child subtree.
 * @param field_no Field number of @a child.
 * @param header Beginning of the array, MessagePack header.
 *
 * @retval  0 Success.
 * @retval -1 Error.
 */
int
update_array_create_with_child(struct update_field *field,
			       const struct update_field *child,
			       int32_t field_no, const char *header);

OP_DECL_GENERIC(array)

/* }}} update_field.array */

/* {{{ update_field.bar */

OP_DECL_GENERIC(bar)

/* }}} update_field.bar */

/* {{{ update_field.nop */

OP_DECL_GENERIC(nop)

/* }}} update_field.nop */

/* {{{ update_field.route */

/**
 * Take a bar or a route @a field and split its path in the place
 * where @a new_op should be applied. Prefix becomes a new route,
 * suffix becomes a child of the result field. In the result @a
 * field stays root of its subtree, and a node of that subtree
 * is returned, to which @a new_op should be applied.
 *
 * Note, this function does not apply @a new_op. It just finds to
 * where it should be applied and does all preparations. It is
 * done deliberately, because otherwise do_cb() virtual function
 * of @a new_op would have been called here, since there is no
 * context. But a caller always knows exactly if it was insert,
 * set, arith, etc. And a caller can and does use more specific
 * function like do_op_set, do_op_insert ...
 *
 * @param field Field to find where to apply @a new_op.
 * @param new_op New operation to apply.
 * @return A field to which @a new_op should be applied.
 */
struct update_field *
update_route_branch(struct update_field *field, struct update_op *new_op);

OP_DECL_GENERIC(route)

/* }}} update_field.route */

#undef OP_DECL_GENERIC

/* {{{ Common helpers. */

/**
 * These helper functions are used when a caller function doesn't
 * know type of a child node to propagate an update down the tree.
 * For example, do_op_array_set calls do_op_set on its childs.
 * Each child can be another array, a bar, a route, a map -
 * anything. The functions below help to make such places shorter
 * and simpler.
 *
 * Note, that they are recursive, although it is not clearly
 * visible. For example, if an update tree contains several array
 * nodes on one tree branch, then update of the deepest array goes
 * through each of these nodes and calls do_op_array_<opname> on
 * needed children. But it is ok, because operation count is
 * usually small (<<50 in the most cases, usually <= 5), and the
 * update tree hight is not bigger than operation count. Also,
 * fiber stack is big enough to fit ~10k update tree depth -
 * incredible number, even though the real limit is 4k due to
 * limited number of operations.
 */
#define OP_DECL_GENERIC(op_type)						\
static inline int								\
do_op_##op_type(struct update_op *op, struct update_field *field)		\
{										\
	switch (field->type) {							\
	case UPDATE_ARRAY:							\
		return do_op_array_##op_type(op, field);			\
	case UPDATE_NOP:							\
		return do_op_nop_##op_type(op, field);				\
	case UPDATE_BAR:							\
		return do_op_bar_##op_type(op, field);				\
	case UPDATE_ROUTE:							\
		return do_op_route_##op_type(op, field);			\
	default:								\
		unreachable();							\
	}									\
	return 0;								\
}

OP_DECL_GENERIC(insert)

OP_DECL_GENERIC(set)

OP_DECL_GENERIC(delete)

OP_DECL_GENERIC(arith)

OP_DECL_GENERIC(bit)

OP_DECL_GENERIC(splice)

#undef OP_DECL_GENERIC

/* }}} Common helpers. */

/* {{{ Scalar helpers. */

int
make_arith_operation(struct update_op *op, struct op_arith_arg arg,
		     struct op_arith_arg *ret);

void
store_op_arith(struct update_op *op, const char *in, char *out);

uint32_t
update_arith_sizeof(struct op_arith_arg *arg);

int
update_op_do_arith(struct update_op *op, const char *old);

int
update_op_do_bit(struct update_op *op, const char *old);

int
update_op_do_splice(struct update_op *op, const char *old);

/* }}} Scalar helpers. */

/** {{{ Error helpers. */
/**
 * All the error helpers below set diag with appropriate error
 * code, taking into account field_no < 0, complex paths. They all
 * return -1 to shorten error returning in a caller function to
 * single line.
 */

int
update_err_no_such_field(const struct update_op *op);

int
update_err(const struct update_op *op, const char *reason);

static inline int
update_err_double(const struct update_op *op)
{
	return update_err(op, "double update of the same field");
}

static inline int
update_err_bad_json(const struct update_op *op, int pos)
{
	return update_err(op, tt_sprintf("invalid JSON in position %d", pos));
}

static inline int
update_err_delete1(const struct update_op *op)
{
	return update_err(op, "can delete only 1 field from a map in a row");
}

static inline int
update_err_duplicate(const struct update_op *op)
{
	return update_err(op, "the key exists already");
}

/** }}} Error helpers. */

#endif /* TARANTOOL_BOX_TUPLE_UPDATE_FIELD_H */
