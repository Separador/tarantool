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
#include "box/tuple.h"

/**
 * Locate the field to update by JSON path in @a op->path. If
 * found, initialize @a field as a bar update.
 * @param op Update operation.
 * @param field Field to locate in.
 *
 * @retval 0 Success.
 * @retval -1 Not found or invalid JSON.
 */
static inline int
update_bar_locate(struct update_op *op, struct update_field *field)
{
	assert(! update_op_is_term(op));
	const char *parent = NULL, *pos = field->data;
	field->bar.path = op->lexer.src + op->lexer.offset;
	field->bar.path_len = op->lexer.src_len - op->lexer.offset;
	int rc;
	struct json_token token;
	while ((rc = json_lexer_next_token(&op->lexer, &token)) == 0 &&
	       token.type != JSON_TOKEN_END) {

		parent = pos;
		switch (token.type) {
		case JSON_TOKEN_NUM:
			rc = tuple_field_go_to_index(&pos, token.num);
			break;
		case JSON_TOKEN_STR:
			rc = tuple_field_go_to_key(&pos, token.str, token.len);
			break;
		default:
			assert(token.type == JSON_TOKEN_ANY);
			return update_err_bad_json(op,
						   op->lexer.symbol_count - 1);
		}
		if (rc != 0)
			return update_err_no_such_field(op);
	}
	if (rc > 0)
		return update_err_bad_json(op, rc);

	field->type = UPDATE_BAR;
	field->bar.point = pos;
	mp_next(&pos);
	field->bar.point_size = pos - field->bar.point;
	field->bar.op = op;
	field->bar.parent = parent;
	return 0;
}

/**
 * Locate the optional field to set by JSON path in @a op->path.
 * If found or only a last path part is not found, initialize @a
 * field.
 * @param op Update operation.
 * @param field Field to locate in.
 * @param[out] is_found Set if the field was found.
 * @param[out] key_len_or_index One parameter for two values,
 *        depending on where the target point is located: in an
 *        array or a map. In case of map it is size of a key
 *        before the found point. It is used to find range of the
 *        both key and value in '#' operation to drop the pair.
 *        In case of array it is index of the point to be able to
 *        check how many fields are left for deletion.
 *
 * @retval 0 Success.
 * @retval -1 Not found non-last path part or invalid JSON.
 */
static inline int
update_bar_locate_opt(struct update_op *op, struct update_field *field,
		      bool *is_found, int *key_len_or_index)
{
	assert(! update_op_is_term(op));
	int rc;
	field->type = UPDATE_BAR;
	field->bar.op = op;
	field->bar.path = op->lexer.src + op->lexer.offset;
	field->bar.path_len = op->lexer.src_len - op->lexer.offset;
	const char *pos = field->data;
	struct json_token token;
	do {
		rc = json_lexer_next_token(&op->lexer, &token);
		if (rc != 0)
			return update_err_bad_json(op, rc);

		switch (token.type) {
		case JSON_TOKEN_END:
			*is_found = true;
			field->bar.point = pos;
			mp_next(&pos);
			field->bar.point_size = pos - field->bar.point;
			return 0;
		case JSON_TOKEN_NUM:
			field->bar.parent = pos;
			*key_len_or_index = token.num;
			rc = tuple_field_go_to_index(&pos, token.num);
			break;
		case JSON_TOKEN_STR:
			field->bar.parent = pos;
			*key_len_or_index = token.len;
			rc = tuple_field_go_to_key(&pos, token.str, token.len);
			break;
		default:
			assert(token.type == JSON_TOKEN_ANY);
			return update_err_bad_json(op,
						   op->lexer.symbol_count - 1);
		}
	} while (rc == 0);
	assert(rc == -1);
	struct json_token tmp_token;
	rc = json_lexer_next_token(&op->lexer, &tmp_token);
	if (rc != 0)
		return update_err_bad_json(op, rc);
	if (tmp_token.type != JSON_TOKEN_END)
		return update_err_no_such_field(op);

	*is_found = false;
	if (token.type == JSON_TOKEN_NUM) {
		if (mp_typeof(*field->bar.parent) != MP_ARRAY) {
			return update_err(op, "can not access by index a "\
					  "non-array field");
		}
		const char *tmp = field->bar.parent;
		uint32_t size = mp_decode_array(&tmp);
		if ((uint32_t) token.num > size)
			return update_err_no_such_field(op);
		/*
		 * The only way not to find an element in an array
		 * by an index is to use array size as the index.
		 */
		assert((uint32_t) token.num == size);
		if (field->bar.parent == field->data) {
			field->bar.point = field->data + field->size;
		} else {
			field->bar.point = field->bar.parent;
			mp_next(&field->bar.point);
		}
	} else {
		assert(token.type == JSON_TOKEN_STR);
		field->bar.new_key = token.str;
		field->bar.new_key_len = token.len;
		if (mp_typeof(*field->bar.parent) != MP_MAP) {
			return update_err(op, "can not access by key a "\
					  "non-map field");
		}
	}
	return 0;
}

int
do_op_nop_insert(struct update_op *op, struct update_field *field)
{
	assert(op->opcode == '!');
	assert(field->type == UPDATE_NOP);
	bool is_found = false;
	int key_len = 0;
	if (update_bar_locate_opt(op, field, &is_found, &key_len) != 0)
		return -1;
	op->new_field_len = op->arg.set.length;
	if (mp_typeof(*field->bar.parent) == MP_MAP) {
		if (is_found)
			return update_err_duplicate(op);
		op->new_field_len += mp_sizeof_str(key_len);
	}
	return 0;
}

int
do_op_nop_set(struct update_op *op, struct update_field *field)
{
	assert(op->opcode == '=');
	assert(field->type == UPDATE_NOP);
	bool is_found = false;
	int key_len = 0;
	if (update_bar_locate_opt(op, field, &is_found, &key_len) != 0)
		return -1;
	op->new_field_len = op->arg.set.length;
	if (! is_found) {
		op->opcode = '!';
		if (mp_typeof(*field->bar.parent) == MP_MAP)
			op->new_field_len += mp_sizeof_str(key_len);
	}
	return 0;
}

int
do_op_nop_delete(struct update_op *op, struct update_field *field)
{
	assert(op->opcode == '#');
	assert(field->type == UPDATE_NOP);
	bool is_found = false;
	int key_len_or_index = 0;
	if (update_bar_locate_opt(op, field, &is_found, &key_len_or_index) != 0)
		return -1;
	if (! is_found)
		return update_err_no_such_field(op);
	if (mp_typeof(*field->bar.parent) == MP_ARRAY) {
		const char *tmp = field->bar.parent;
		uint32_t size = mp_decode_array(&tmp);
		if (key_len_or_index + op->arg.del.count > size)
			op->arg.del.count = size - key_len_or_index;
		const char *end = field->bar.point + field->bar.point_size;
		for (uint32_t i = 1; i < op->arg.del.count; ++i)
			mp_next(&end);
		field->bar.point_size = end - field->bar.point;
	} else {
		if (op->arg.del.count != 1)
			return update_err_delete1(op);
		/* Take key size into account to delete it too. */
		uint32_t key_size = mp_sizeof_str(key_len_or_index);
		field->bar.point -= key_size;
		field->bar.point_size += key_size;
	}
	return 0;
}

#define DO_SCALAR_OP_GENERIC(op_type)						\
int										\
do_op_bar_##op_type(struct update_op *op, struct update_field *field)		\
{										\
	(void) op;								\
	(void) field;								\
	assert(field->type == UPDATE_BAR);					\
	diag_set(ClientError, ER_UNSUPPORTED, "update",				\
		 "intersected JSON paths");					\
	return -1;								\
}

DO_SCALAR_OP_GENERIC(insert)

DO_SCALAR_OP_GENERIC(set)

DO_SCALAR_OP_GENERIC(delete)

DO_SCALAR_OP_GENERIC(arith)

DO_SCALAR_OP_GENERIC(bit)

DO_SCALAR_OP_GENERIC(splice)

#undef DO_SCALAR_OP_GENERIC

#define DO_SCALAR_OP_GENERIC(op_type)						\
int										\
do_op_nop_##op_type(struct update_op *op, struct update_field *field)		\
{										\
	assert(field->type == UPDATE_NOP);					\
	if (update_bar_locate(op, field) != 0)					\
		return -1;							\
	return update_op_do_##op_type(op, field->bar.point);			\
}

DO_SCALAR_OP_GENERIC(arith)

DO_SCALAR_OP_GENERIC(bit)

DO_SCALAR_OP_GENERIC(splice)

uint32_t
update_bar_sizeof(struct update_field *field)
{
	assert(field->type == UPDATE_BAR);
	switch(field->bar.op->opcode) {
	case '!': {
		const char *parent = field->bar.parent;
		uint32_t size = field->size + field->bar.op->new_field_len;
		if (mp_typeof(*parent) == MP_ARRAY) {
			uint32_t array_size = mp_decode_array(&parent);
			return size + mp_sizeof_array(array_size + 1) -
			       mp_sizeof_array(array_size);
		} else {
			uint32_t map_size = mp_decode_map(&parent);
			return size + mp_sizeof_map(map_size + 1) -
			       mp_sizeof_map(map_size);
		}
	}
	case '#': {
		const char *parent = field->bar.parent;
		uint32_t delete_count = field->bar.op->arg.del.count;
		uint32_t size = field->size - field->bar.point_size;
		if (mp_typeof(*parent) == MP_ARRAY) {
			uint32_t array_size = mp_decode_array(&parent);
			assert(array_size >= delete_count);
			return size - mp_sizeof_array(array_size) +
			       mp_sizeof_array(array_size - delete_count);
		} else {
			uint32_t map_size = mp_decode_map(&parent);
			assert(delete_count == 1);
			return size - mp_sizeof_map(map_size) +
			       mp_sizeof_map(map_size - 1);
		}
	}
	default: {
		return field->size - field->bar.point_size +
		       field->bar.op->new_field_len;
	}
	}
}

uint32_t
update_bar_store(struct update_field *field, char *out, char *out_end)
{
	assert(field->type == UPDATE_BAR);
	(void) out_end;
	struct update_op *op = field->bar.op;
	char *out_saved = out;
	switch(op->opcode) {
	case '!': {
		const char *pos = field->bar.parent;
		uint32_t before_parent = pos - field->data;
		/* Before parent. */
		memcpy(out, field->data, before_parent);
		out += before_parent;
		if (mp_typeof(*pos) == MP_ARRAY) {
			/* New array header. */
			uint32_t size = mp_decode_array(&pos);
			out = mp_encode_array(out, size + 1);
			/* Before insertion point. */
			size = field->bar.point - pos;
			memcpy(out, pos, size);
			out += size;
			pos += size;
		} else {
			/* New map header. */
			uint32_t size = mp_decode_map(&pos);
			out = mp_encode_map(out, size + 1);
			/* New key. */
			out = mp_encode_str(out, field->bar.new_key,
					    field->bar.new_key_len);
		}
		/* New value. */
		memcpy(out, op->arg.set.value, op->arg.set.length);
		out += op->arg.set.length;
		/* Old values and field tail. */
		uint32_t after_point = field->data + field->size - pos;
		memcpy(out, pos, after_point);
		out += after_point;
		return out - out_saved;
	}
	case '#': {
		const char *pos = field->bar.parent;
		uint32_t size, before_parent = pos - field->data;
		memcpy(out, field->data, before_parent);
		out += before_parent;
		if (mp_typeof(*pos) == MP_ARRAY) {
			size = mp_decode_array(&pos);
			out = mp_encode_array(out, size - op->arg.del.count);
		} else {
			size = mp_decode_map(&pos);
			out = mp_encode_map(out, size - 1);
		}
		size = field->bar.point - pos;
		memcpy(out, pos, size);
		out += size;
		pos = field->bar.point + field->bar.point_size;

		size = field->data + field->size - pos;
		memcpy(out, pos, size);
		return out + size - out_saved;
	}
	default: {
		uint32_t before_point = field->bar.point - field->data;
		const char *field_end = field->data + field->size;
		const char *point_end =
			field->bar.point + field->bar.point_size;
		uint32_t after_point = field_end - point_end;

		memcpy(out, field->data, before_point);
		out += before_point;
		op->meta->store_cb(op, field->bar.point, out);
		out += op->new_field_len;
		memcpy(out, point_end, after_point);
		return out + after_point - out_saved;
	}
	}
}
