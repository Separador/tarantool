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
#include "update_field.h"
#include "msgpuck.h"
#include "fiber.h"

/**
 * Make sure @a op contains a valid field number. It can be not
 * so, if the array's parent didn't propagate operation's lexer.
 * In fact, the parent fills fieldno only in some rare cases like
 * branching. Generally, an array should care about fieldno by
 * itself.
 */
static inline int
update_op_prepare_num_token(struct update_op *op)
{
	if (op->token_type == JSON_TOKEN_END &&
	    update_op_consume_token(op) != 0)
			return -1;
	if (op->token_type != JSON_TOKEN_NUM) {
		return update_err(op, "can't update an array by not a "\
				  "number index");
	}
	return 0;
}

/**
 * Make field number positive and check for the field existence.
 * @param op Update operation.
 * @param field_count Field count in the array having this field.
 *
 * @retval 0 Success.
 * @retval -1 Error.
 */
static inline int
update_op_adjust_field_no(struct update_op *op, int32_t field_count)
{
	assert(op->token_type == JSON_TOKEN_NUM);
	if (op->field_no >= 0) {
		if (op->field_no < field_count)
			return 0;
		return update_err_no_such_field(op);
	}
	if (op->field_no + field_count >= 0) {
		op->field_no += field_count;
		return 0;
	}
	return update_err_no_such_field(op);
}

/**
 * Update of an array consists of array items. Each item is a
 * range of fields. Item updates first field of the range and
 * stores size of others to save them with no changes into a new
 * tuple later. It allows on update of a single field in an array
 * store at most 2 objects - item for the previous fields, and
 * item for this one + its tail. This is how rope data structure
 * works.
 */
struct update_array_item {
	/** Update of the first field in the range. */
	struct update_field field;
	/** Size of other fields in the range. */
	uint32_t tail_size;
};

/** Initialize an array item. */
static inline void
update_array_item_create(struct update_array_item *item, enum update_type type,
			 const char *data, uint32_t size, uint32_t tail_size)
{
	item->field.type = type;
	item->field.data = data;
	item->field.size = size;
	item->tail_size = tail_size;
}

/** Rope allocator for nodes, paths, items etc. */
static inline void *
rope_alloc(struct region *region, size_t size)
{
	void *ptr = region_aligned_alloc(region, size, alignof(uint64_t));
	if (ptr == NULL) {
		diag_set(OutOfMemory, size, "region_aligned_alloc",
			 "update internals");
	}
	return ptr;
}

/** Split a range of fields in two. */
static struct update_array_item *
rope_field_split(struct region *region, struct update_array_item *prev,
		 size_t size, size_t offset)
{
	(void) size;
	struct update_array_item *next =
		(struct update_array_item *) rope_alloc(region, sizeof(*next));
	if (next == NULL)
		return NULL;
	assert(offset > 0 && prev->tail_size > 0);

	const char *tail = prev->field.data + prev->field.size;
	const char *field = tail;
	const char *range_end = tail + prev->tail_size;

	for (uint32_t i = 1; i < offset; ++i)
		mp_next(&field);

	prev->tail_size = field - tail;
	const char *field_end = field;
	mp_next(&field_end);
	update_array_item_create(next, UPDATE_NOP, field, field_end - field,
				 range_end - field_end);
	return next;
}

#define ROPE_SPLIT_F rope_field_split
#define ROPE_ALLOC_F rope_alloc
#define rope_data_t struct update_array_item *
#define rope_ctx_t struct region *

#include "salad/rope.h"

/**
 * Extract from the array an item whose range starts from the
 * field affected by @a op.
 */
static inline struct update_array_item *
update_array_extract_item(struct update_field *field, struct update_op *op)
{
	assert(field->type == UPDATE_ARRAY);
	struct rope *rope = field->array.rope;
	if (update_op_adjust_field_no(op, rope_size(rope)) != 0)
		return NULL;
	return rope_extract(rope, op->field_no);
}

int
update_array_create(struct update_field *field, const char *header,
		    const char *data, const char *data_end,
		    uint32_t field_count)
{
	field->type = UPDATE_ARRAY;
	field->data = header;
	field->size = data_end - header;
	field->array.rope = rope_new(&fiber()->gc);
	if (field->array.rope == NULL)
		return -1;
	struct update_array_item *item = (struct update_array_item *)
		rope_alloc(&fiber()->gc, sizeof(*item));
	if (item == NULL)
		return -1;
	if (data == data_end)
		return 0;
	/*
	 * Initial item consists of one range - the whole array.
	 */
	const char *begin = data;
	mp_next(&data);
	update_array_item_create(item, UPDATE_NOP, begin, data - begin,
				 data_end - data);
	return rope_append(field->array.rope, item, field_count);
}

int
update_array_create_with_child(struct update_field *field,
			       const struct update_field *child,
			       int32_t field_no, const char *header)
{
	const char *data = header;
	uint32_t field_count = mp_decode_array(&data);
	const char *first_field = data;
	const char *first_field_end = first_field;
	mp_next(&first_field_end);
	struct region *region = &fiber()->gc;
	struct rope *rope = rope_new(region);
	if (rope == NULL)
		return -1;
	struct update_array_item *item =
		(struct update_array_item *) rope_alloc(region, sizeof(*item));
	if (item == NULL)
		return -1;
	const char *end = first_field_end;
	if (field_no > 0) {
		for (int32_t i = 1; i < field_no; ++i)
			mp_next(&end);
		update_array_item_create(item, UPDATE_NOP, first_field,
					 first_field_end - first_field,
					 end - first_field_end);
		if (rope_append(rope, item, field_no) != 0)
			return -1;
		item = (struct update_array_item *) rope_alloc(region,
							       sizeof(*item));
		if (item == NULL)
			return -1;
		first_field = end;
		first_field_end = first_field;
		mp_next(&first_field_end);
		end = first_field_end;
	}
	for (uint32_t i = field_no + 1; i < field_count; ++i)
		mp_next(&end);
	item->field = *child;
	update_array_item_create(item, child->type, first_field,
				 first_field_end - first_field,
				 end - first_field_end);
	field->type = UPDATE_ARRAY;
	field->data = header;
	field->size = end - header;
	field->array.rope = rope;
	return rope_append(rope, item, field_count - field_no);
}

uint32_t
update_array_sizeof(struct update_field *field)
{
	assert(field->type == UPDATE_ARRAY);
	struct rope_iter it;
	rope_iter_create(&it, field->array.rope);

	uint32_t res = mp_sizeof_array(rope_size(field->array.rope));
	for (struct rope_node *node = rope_iter_start(&it); node != NULL;
	     node = rope_iter_next(&it)) {
		struct update_array_item *item = rope_leaf_data(node);
		res += update_field_sizeof(&item->field) + item->tail_size;
	}
	return res;
}

uint32_t
update_array_store(struct update_field *field, char *out, char *out_end)
{
	assert(field->type == UPDATE_ARRAY);
	char *out_begin = out;
	out = mp_encode_array(out, rope_size(field->array.rope));
	uint32_t total_field_count = 0;
	struct rope_iter it;
	rope_iter_create(&it, field->array.rope);
	for (struct rope_node *node = rope_iter_start(&it); node != NULL;
	     node = rope_iter_next(&it)) {
		struct update_array_item *item = rope_leaf_data(node);
		uint32_t field_count = rope_leaf_size(node);
		out += update_field_store(&item->field, out, out_end);
		assert(item->tail_size == 0 || field_count > 1);
		memcpy(out, item->field.data + item->field.size,
		       item->tail_size);
		out += item->tail_size;
		total_field_count += field_count;
	}
	(void) total_field_count;
	assert(rope_size(field->array.rope) == total_field_count);
	assert(out <= out_end);
	return out - out_begin;
}

int
do_op_array_insert(struct update_op *op, struct update_field *field)
{
	assert(field->type == UPDATE_ARRAY);
	struct rope *rope = field->array.rope;
	struct update_array_item *item;
	if (update_op_prepare_num_token(op) != 0)
		return -1;

	if (! update_op_is_term(op)) {
		item = update_array_extract_item(field, op);
		if (item == NULL)
			return -1;
		op->token_type = JSON_TOKEN_END;
		return do_op_insert(op, &item->field);
	}

	if (update_op_adjust_field_no(op, rope_size(rope) + 1) != 0)
		return -1;

	item = (struct update_array_item *) rope_alloc(rope->ctx,
						       sizeof(*item));
	if (item == NULL)
		return -1;
	update_array_item_create(item, UPDATE_NOP, op->arg.set.value,
				 op->arg.set.length, 0);
	return rope_insert(rope, op->field_no, item, 1);
}

int
do_op_array_set(struct update_op *op, struct update_field *field)
{
	assert(field->type == UPDATE_ARRAY);
	struct rope *rope = field->array.rope;
	if (update_op_prepare_num_token(op) != 0)
		return -1;

	/* Interpret '=' for n + 1 field as insert. */
	if (op->field_no == (int32_t) rope_size(rope))
		return do_op_array_insert(op, field);

	struct update_array_item *item =
		update_array_extract_item(field, op);
	if (item == NULL)
		return -1;
	if (! update_op_is_term(op)) {
		op->token_type = JSON_TOKEN_END;
		return do_op_set(op, &item->field);
	}
	op->new_field_len = op->arg.set.length;
	/* Ignore the previous op, if any. */
	item->field.type = UPDATE_SCALAR;
	item->field.scalar.op = op;
	return 0;
}

int
do_op_array_delete(struct update_op *op, struct update_field *field)
{
	assert(field->type == UPDATE_ARRAY);
	if (update_op_prepare_num_token(op) != 0)
		return -1;

	if (! update_op_is_term(op)) {
		struct update_array_item *item =
			update_array_extract_item(field, op);
		if (item == NULL)
			return -1;
		op->token_type = JSON_TOKEN_END;
		return do_op_delete(op, &item->field);
	}
	struct rope *rope = field->array.rope;
	uint32_t size = rope_size(rope);
	if (update_op_adjust_field_no(op, size) != 0)
		return -1;
	uint32_t delete_count = op->arg.del.count;
	if ((uint64_t) op->field_no + delete_count > size)
		delete_count = size - op->field_no;
	for (uint32_t u = delete_count; u != 0; --u)
		rope_erase(rope, op->field_no);
	return 0;
}

#define DO_SCALAR_OP_GENERIC(op_type)						\
int										\
do_op_array_##op_type(struct update_op *op, struct update_field *field)		\
{										\
	if (update_op_prepare_num_token(op) != 0)				\
		return -1;							\
	struct update_array_item *item =					\
		update_array_extract_item(field, op);				\
	if (item == NULL)							\
		return -1;							\
	if (! update_op_is_term(op)) {						\
		op->token_type = JSON_TOKEN_END;				\
		return do_op_##op_type(op, &item->field);			\
	}									\
	if (item->field.type != UPDATE_NOP)					\
		return update_err_double(op);					\
	if (update_op_do_##op_type(op, item->field.data) != 0)			\
		return -1;							\
	item->field.type = UPDATE_SCALAR;					\
	item->field.scalar.op = op;						\
	return 0;								\
}

DO_SCALAR_OP_GENERIC(arith)

DO_SCALAR_OP_GENERIC(bit)

DO_SCALAR_OP_GENERIC(splice)
