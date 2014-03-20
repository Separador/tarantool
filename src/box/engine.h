#ifndef TARANTOOL_BOX_ENGINE_H_INCLUDED
#define TARANTOOL_BOX_ENGINE_H_INCLUDED
/*
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
#include "tuple.h"
#include "index.h"
#include <exception.h>

struct tuple;
struct space;

typedef void (*engine_recover_f)(struct space *space);

typedef struct tuple*
(*engine_replace_f)(struct space *space,
                    struct tuple *old_tuple,
                    struct tuple *new_tuple, enum dup_replace_mode);

/** Reflects what space_replace() is supposed to do. */
enum engine_recovery_state {
	/**
	 * The space is created, but has no data
	 * and no primary key, or, if there is a primary
	 * key, it's not ready for use (being built with
	 * buildNext()).
	 * Replace is always an error, since there are no
	 * indexes to add data to.
	 */
	READY_NO_KEYS,
	/**
	 * The space has a functional primary key.
	 * Replace adds the tuple to this key.
	 */
	READY_PRIMARY_KEY,
	/**
	 * The space is fully functional, all keys
	 * are fully built, replace adds its tuple
	 * to all keys.
	 */
	READY_ALL_KEYS
};

struct engine {
	const char *name;
	struct engine *origin;
	enum engine_recovery_state state;
	/* Recover is called after each recover step to enable
	 * keys. When recovery is complete, it enables all keys
	 * at once and resets itself to a no-op.
	 */
	engine_recover_f recover;
	engine_replace_f replace;

	void (*init)(struct engine*);
	void (*free)(struct engine*);

	struct rlist link;
};

extern struct rlist engines;

/** Register engine instance. */
void engine_register(struct engine *engine);

/** Find engine by name. */
struct engine *engine_find(const char *name);

/** Init engine instance. */
void engine_init(struct engine *instance, const char *name);

/** Shutdown all engines. */
void engine_shutdown();

/** Call a visitor function on every registered engine. */
void engine_foreach(void (*func)(struct engine *engine, void *udata),
                    void *udata);

/**
 * Derive recovery state from a parent engine
 * handler.
 */
void engine_derive(struct engine *engine);

extern struct engine engine_memtx;

#endif /* TARANTOOL_BOX_ENGINE_H_INCLUDED */
