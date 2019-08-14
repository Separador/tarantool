/*
 * Copyright 2010-2016, Tarantool AUTHORS, please see AUTHORS file.
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
#include "cbus.h"

#include <limits.h>
#include "fiber.h"
#include "trigger.h"

/**
 * Cord interconnect.
 */
struct cbus {
	/** cbus statistics */
	struct rmean *stats;
	/** A mutex to protect bus join. */
	pthread_mutex_t mutex;
	/** Condition for synchronized start of the bus. */
	pthread_cond_t cond;
	/** Connected endpoints */
	struct rlist endpoints;
};

/** A singleton for all cords. */
static struct cbus cbus;

const char *cbus_stat_strings[CBUS_STAT_LAST] = {
	"EVENTS",
	"LOCKS",
};

/**
 * Find a joined cbus endpoint by name.
 * This is an internal helper method which should be called
 * under cbus::mutex.
 *
 * @return endpoint or NULL if not found
 */
static struct cbus_endpoint *
cbus_find_endpoint_locked(struct cbus *bus, const char *name)
{
	struct cbus_endpoint *endpoint;
	rlist_foreach_entry(endpoint, &bus->endpoints, in_cbus) {
		if (strcmp(endpoint->name, name) == 0)
			return endpoint;
	}
	return NULL;
}

static struct cbus_endpoint *
cbus_find_endpoint(struct cbus *bus, const char *name)
{
	tt_pthread_mutex_lock(&bus->mutex);
	struct cbus_endpoint *endpoint = cbus_find_endpoint_locked(bus, name);
	tt_pthread_mutex_unlock(&bus->mutex);
	return endpoint;
}

static void
cpipe_flush_cb(ev_loop * /* loop */, struct ev_async *watcher,
	       int /* events */);

void
cpipe_create(struct cpipe *pipe, const char *consumer)
{
	stailq_create(&pipe->input);

	pipe->n_input = 0;
	pipe->max_input = INT_MAX;
	pipe->producer = cord()->loop;

	ev_async_init(&pipe->flush_input, cpipe_flush_cb);
	pipe->flush_input.data = pipe;
	rlist_create(&pipe->on_flush);

	tt_pthread_mutex_lock(&cbus.mutex);
	struct cbus_endpoint *endpoint =
		cbus_find_endpoint_locked(&cbus, consumer);
	while (endpoint == NULL) {
		tt_pthread_cond_wait(&cbus.cond, &cbus.mutex);
		endpoint = cbus_find_endpoint_locked(&cbus, consumer);
	}
	pipe->endpoint = endpoint;
	++pipe->endpoint->n_pipes;
	tt_pthread_mutex_unlock(&cbus.mutex);
}

struct cmsg_poison {
	struct cmsg msg;
	struct cbus_endpoint *endpoint;
};

static void
cbus_endpoint_poison_f(struct cmsg *msg)
{
	struct cbus_endpoint *endpoint = ((struct cmsg_poison *)msg)->endpoint;
	tt_pthread_mutex_lock(&cbus.mutex);
	assert(endpoint->n_pipes > 0);
	--endpoint->n_pipes;
	tt_pthread_mutex_unlock(&cbus.mutex);
	fiber_cond_signal(&endpoint->cond);
	free(msg);
}

void
cpipe_destroy(struct cpipe *pipe)
{
	ev_async_stop(pipe->producer, &pipe->flush_input);

	trigger_destroy(&pipe->on_flush);

	struct cbus_endpoint *endpoint = pipe->endpoint;
	struct cmsg_poison *poison = malloc(sizeof(struct cmsg_poison));
	poison->msg.f = cbus_endpoint_poison_f;
	poison->endpoint = pipe->endpoint;
	/*
	 * Avoid the general purpose cpipe_push_input() since
	 * we want to control the way the poison message is
	 * delivered.
	 */
	tt_pthread_mutex_lock(&endpoint->mutex);
	/* Flush input */
	stailq_concat(&endpoint->output, &pipe->input);
	pipe->n_input = 0;
	/* Add the pipe shutdown message as the last one. */
	stailq_add_tail_entry(&endpoint->output, poison, msg.fifo);
	/* Count statistics */
	rmean_collect(cbus.stats, CBUS_STAT_EVENTS, 1);
	/*
	 * Keep the lock for the duration of ev_async_send():
	 * this will avoid a race condition between
	 * ev_async_send() and execution of the poison
	 * message, after which the endpoint may disappear.
	 */
	ev_async_send(endpoint->consumer, &endpoint->async);
	tt_pthread_mutex_unlock(&endpoint->mutex);

	TRASH(pipe);
}

static void
cbus_create(struct cbus *bus)
{
	bus->stats = rmean_new(cbus_stat_strings, CBUS_STAT_LAST);
	if (bus->stats == NULL)
		panic_syserror("cbus_create");

	/* Initialize queue lock mutex. */
	(void) tt_pthread_mutex_init(&bus->mutex, NULL);

	(void) tt_pthread_cond_init(&bus->cond, NULL);

	rlist_create(&bus->endpoints);
}

static void
cbus_destroy(struct cbus *bus)
{
	/*
	 * Lock the mutex to ensure we do not destroy a mutex
	 * while it is locked, happens in at_exit() handler.
	 */
	(void) tt_pthread_mutex_lock(&bus->mutex);
	(void) tt_pthread_mutex_unlock(&bus->mutex);
	(void) tt_pthread_mutex_destroy(&bus->mutex);
	(void) tt_pthread_cond_destroy(&bus->cond);
	rmean_delete(bus->stats);
}

/**
 * Join a new endpoint (message consumer) to the bus. The endpoint
 * must have a unique name. Wakes up all producers (@sa cpipe_create())
 * who are blocked waiting for this endpoint to become available.
 */
int
cbus_endpoint_create(struct cbus_endpoint *endpoint, const char *name,
		     void (*fetch_cb)(ev_loop *, struct ev_watcher *, int),
		     void *fetch_data)
{
	tt_pthread_mutex_lock(&cbus.mutex);
	if (cbus_find_endpoint_locked(&cbus, name) != NULL) {
		tt_pthread_mutex_unlock(&cbus.mutex);
		return 1;
	}

	snprintf(endpoint->name, sizeof(endpoint->name), "%s", name);
	endpoint->consumer = loop();
	endpoint->n_pipes = 0;
	fiber_cond_create(&endpoint->cond);
	tt_pthread_mutex_init(&endpoint->mutex, NULL);
	stailq_create(&endpoint->output);
	ev_async_init(&endpoint->async,
		      (void (*)(ev_loop *, struct ev_async *, int)) fetch_cb);
	endpoint->async.data = fetch_data;
	ev_async_start(endpoint->consumer, &endpoint->async);

	rlist_add_tail(&cbus.endpoints, &endpoint->in_cbus);
	tt_pthread_mutex_unlock(&cbus.mutex);
	/*
	 * Alert all waiting producers.
	 *
	 * POSIX: pthread_cond_broadcast() function shall
	 * have no effect if there are no threads currently
	 * blocked on cond.
	 */
	tt_pthread_cond_broadcast(&cbus.cond);
	return 0;
}

int
cbus_endpoint_destroy(struct cbus_endpoint *endpoint,
		      void (*process_cb)(struct cbus_endpoint *endpoint))
{
	tt_pthread_mutex_lock(&cbus.mutex);
	/*
	 * Remove endpoint from cbus registry, so no new pipe can
	 * be created for this endpoint.
	 */
	rlist_del(&endpoint->in_cbus);
	tt_pthread_mutex_unlock(&cbus.mutex);

	while (true) {
		if (process_cb)
			process_cb(endpoint);
		if (endpoint->n_pipes == 0 && stailq_empty(&endpoint->output))
			break;
		 fiber_cond_wait(&endpoint->cond);
	}

	/*
	 * Pipe flush func can still lock mutex, so just lock and unlock
	 * it.
	 */
	tt_pthread_mutex_lock(&endpoint->mutex);
	tt_pthread_mutex_unlock(&endpoint->mutex);
	tt_pthread_mutex_destroy(&endpoint->mutex);
	ev_async_stop(endpoint->consumer, &endpoint->async);
	fiber_cond_destroy(&endpoint->cond);
	TRASH(endpoint);
	return 0;
}

static void
cpipe_flush_cb(ev_loop *loop, struct ev_async *watcher, int events)
{
	(void) loop;
	(void) events;
	struct cpipe *pipe = (struct cpipe *) watcher->data;
	struct cbus_endpoint *endpoint = pipe->endpoint;
	if (pipe->n_input == 0)
		return;

	trigger_run(&pipe->on_flush, pipe);
	/* Trigger task processing when the queue becomes non-empty. */
	bool output_was_empty;

	tt_pthread_mutex_lock(&endpoint->mutex);
	output_was_empty = stailq_empty(&endpoint->output);
	/** Flush input */
	stailq_concat(&endpoint->output, &pipe->input);
	tt_pthread_mutex_unlock(&endpoint->mutex);

	pipe->n_input = 0;
	if (output_was_empty) {
		/* Count statistics */
		rmean_collect(cbus.stats, CBUS_STAT_EVENTS, 1);

		ev_async_send(endpoint->consumer, &endpoint->async);
	}
}

void
cbus_init()
{
	cbus_create(&cbus);
}

void
cbus_free()
{
	cbus_destroy(&cbus);
}

/**
 * Wake up the caller fiber to reap call results.
 * If the fiber is gone, e.g. in case of call timeout
 * or cancellation, invoke free_cb to free message state.
 */
void
cbus_call_done(struct cmsg *m)
{
	struct cbus_call_msg *msg = (struct cbus_call_msg *)m;
	if (msg->caller == NULL) {
		if (msg->free_cb)
			msg->free_cb(msg);
		return;
	}
	msg->complete = true;
	fiber_wakeup(msg->caller);
}

/**
 * Call the target function and store the results (diag, rc) in
 * struct cbus_call_msg.
 */
void
cbus_call_perform(struct cmsg *m)
{
	struct cbus_call_msg *msg = (struct cbus_call_msg *)m;
	msg->rc = msg->func(msg);
	if (msg->rc)
		diag_move(&fiber()->diag, &msg->diag);
	cpipe_push(msg->caller_pipe, cbus_call_done, m);
}

/**
 * Execute a synchronous call over cbus.
 */
int
cbus_call(struct cpipe *callee, struct cpipe *caller, struct cbus_call_msg *msg,
	cbus_call_f func, cbus_call_f free_cb, double timeout)
{
	int rc;

	diag_create(&msg->diag);
	msg->caller = fiber();
	msg->complete = false;
	msg->caller_pipe = caller;

	msg->func = func;
	msg->free_cb = free_cb;
	msg->rc = 0;

	cpipe_push(callee, cbus_call_perform, cmsg(msg));

	fiber_yield_timeout(timeout);
	if (msg->complete == false) {           /* timed out or cancelled */
		msg->caller = NULL;
		if (fiber_is_cancelled())
			diag_set(FiberIsCancelled);
		else
			diag_set(TimedOut);
		return -1;
	}
	if ((rc = msg->rc))
		diag_move(&msg->diag, &fiber()->diag);
	return rc;
}

struct cbus_flush_msg {
	struct cmsg cmsg;
	struct cpipe *caller_pipe;
	bool complete;
	struct fiber_cond cond;
};

static void
cbus_flush_complete(struct cmsg *cmsg)
{
	struct cbus_flush_msg *msg = container_of(cmsg,
			struct cbus_flush_msg, cmsg);
	msg->complete = true;
	fiber_cond_signal(&msg->cond);
}

static void
cbus_flush_perform(struct cmsg *cmsg)
{
	struct cbus_flush_msg *msg = container_of(cmsg, struct cbus_flush_msg,
						  cmsg);
	cpipe_push(msg->caller_pipe, cbus_flush_complete, cmsg);
}

void
cbus_flush(struct cpipe *callee, struct cpipe *caller,
	   void (*process_cb)(struct cbus_endpoint *endpoint))
{
	struct cbus_flush_msg msg;
	msg.caller_pipe = caller;
	msg.complete = false;
	fiber_cond_create(&msg.cond);

	cpipe_push(callee, cbus_flush_perform, &msg.cmsg);

	while (true) {
		if (process_cb != NULL)
			process_cb(caller->endpoint);
		if (msg.complete)
			break;
		fiber_cond_wait(&msg.cond);
	}
}

struct cbus_pair_msg {
	struct cmsg cmsg;
	void (*pair_cb)(void *);
	void *pair_arg;
	const char *src_name;
	struct cpipe *src_pipe;
	bool complete;
	struct fiber_cond cond;
};

static void
cbus_pair_complete(struct cmsg *cmsg);

static void
cbus_pair_perform(struct cmsg *cmsg)
{
	struct cbus_pair_msg *msg = container_of(cmsg,
			struct cbus_pair_msg, cmsg);
	cpipe_create(msg->src_pipe, msg->src_name);
	if (msg->pair_cb != NULL)
		msg->pair_cb(msg->pair_arg);
	cpipe_push(msg->src_pipe, cbus_pair_complete, cmsg);
}

static void
cbus_pair_complete(struct cmsg *cmsg)
{
	struct cbus_pair_msg *msg = container_of(cmsg,
			struct cbus_pair_msg, cmsg);
	msg->complete = true;
	fiber_cond_signal(&msg->cond);
}

void
cbus_pair(const char *dest_name, const char *src_name,
	  struct cpipe *dest_pipe, struct cpipe *src_pipe,
	  void (*pair_cb)(void *), void *pair_arg,
	  void (*process_cb)(struct cbus_endpoint *))
{
	struct cbus_pair_msg msg;

	msg.pair_cb = pair_cb;
	msg.pair_arg = pair_arg;
	msg.complete = false;
	msg.src_name = src_name;
	msg.src_pipe = src_pipe;
	fiber_cond_create(&msg.cond);

	struct cbus_endpoint *endpoint = cbus_find_endpoint(&cbus, src_name);
	assert(endpoint != NULL);

	cpipe_create(dest_pipe, dest_name);
	cpipe_push(dest_pipe, cbus_pair_perform, &msg.cmsg);

	while (true) {
		if (process_cb != NULL)
			process_cb(endpoint);
		if (msg.complete)
			break;
		fiber_cond_wait(&msg.cond);
	}
}

struct cbus_unpair_msg {
	struct cmsg cmsg;
	void (*unpair_cb)(void *);
	void *unpair_arg;
	struct cpipe *src_pipe;
	struct cpipe *dest_pipe;
	bool complete;
	struct fiber_cond cond;
};

static void
cbus_unpair_perform(struct cmsg *cmsg);

static void
cbus_unpair_flush(struct cmsg *cmsg);

static void
cbus_unpair_prepare(struct cmsg *cmsg)
{
	struct cbus_unpair_msg *msg = container_of(cmsg,
			struct cbus_unpair_msg, cmsg);
	if (msg->unpair_cb != NULL)
		msg->unpair_cb(msg->unpair_arg);
	cpipe_push(msg->src_pipe, cbus_unpair_flush, cmsg);
}

static void
cbus_unpair_flush(struct cmsg *cmsg)
{
	struct cbus_unpair_msg *msg = container_of(cmsg,
			struct cbus_unpair_msg, cmsg);
	cpipe_push(msg->dest_pipe, cbus_unpair_perform, cmsg);
}

static void
cbus_unpair_complete(struct cmsg *cmsg);

static void
cbus_unpair_perform(struct cmsg *cmsg)
{
	struct cbus_unpair_msg *msg = container_of(cmsg,
			struct cbus_unpair_msg, cmsg);
	cpipe_push(msg->src_pipe, cbus_unpair_complete, cmsg);
	cpipe_destroy(msg->src_pipe);
}

static void
cbus_unpair_complete(struct cmsg *cmsg)
{
	struct cbus_unpair_msg *msg = container_of(cmsg,
			struct cbus_unpair_msg, cmsg);
	msg->complete = true;
	fiber_cond_signal(&msg->cond);
}

void
cbus_unpair(struct cpipe *dest_pipe, struct cpipe *src_pipe,
	    void (*unpair_cb)(void *), void *unpair_arg,
	    void (*process_cb)(struct cbus_endpoint *))
{
	struct cbus_unpair_msg msg;

	msg.unpair_cb = unpair_cb;
	msg.unpair_arg = unpair_arg;
	msg.src_pipe = src_pipe;
	msg.dest_pipe = dest_pipe;
	msg.complete = false;
	fiber_cond_create(&msg.cond);

	cpipe_push(dest_pipe, cbus_unpair_prepare, &msg.cmsg);

	struct cbus_endpoint *endpoint = src_pipe->endpoint;
	while (true) {
		if (process_cb != NULL)
			process_cb(endpoint);
		if (msg.complete)
			break;
		fiber_cond_wait(&msg.cond);
	}

	cpipe_destroy(dest_pipe);
}

void
cbus_process(struct cbus_endpoint *endpoint)
{
	struct stailq output;
	stailq_create(&output);
	cbus_endpoint_fetch(endpoint, &output);
	struct cmsg *msg, *msg_next;
	stailq_foreach_entry_safe(msg, msg_next, &output, fifo)
		cmsg_deliver(msg);
}

void
cbus_loop(struct cbus_endpoint *endpoint)
{
	while (true) {
		cbus_process(endpoint);
		if (fiber_is_cancelled())
			break;
		fiber_yield();
	}
}

static void
cbus_stop_loop_f(struct cmsg *msg)
{
	fiber_cancel(fiber());
	free(msg);
}

void
cbus_stop_loop(struct cpipe *pipe)
{
	struct cmsg *cancel = malloc(sizeof(struct cmsg));

	cpipe_push(pipe, cbus_stop_loop_f, cancel);
	ev_invoke(pipe->producer, &pipe->flush_input, EV_CUSTOM);
}

