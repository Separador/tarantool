#ifndef TARANTOOL_LIB_CORE_CBUS_H_INCLUDED
#define TARANTOOL_LIB_CORE_CBUS_H_INCLUDED
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
#include "fiber.h"
#include "fiber_cond.h"
#include "rmean.h"
#include "small/rlist.h"
#include "salad/stailq.h"
#include "small/ibuf.h"

#define asm __asm__

#include "cbind.h"

#if defined(__cplusplus)
extern "C" {
#endif /* defined(__cplusplus) */

/** cbus, cmsg - inter-cord bus and messaging */

struct cmsg;
struct cpipe;
typedef void (*cmsg_f)(struct cmsg *);

enum cbus_stat_name {
	CBUS_STAT_EVENTS,
	CBUS_STAT_LOCKS,
	CBUS_STAT_LAST,
};

extern const char *cbus_stat_strings[CBUS_STAT_LAST];

/** A message traveling between cords. */
struct cmsg {
	void *call;
};

struct msg_buf {
	struct stailq_entry entry;
	struct ibuf data;
};

/** A  uni-directional FIFO queue from one cord to another. */
struct cpipe {
	/** Staging area for pushed messages */
	struct slab_cache *slabc;
	struct stailq cache;
	struct stailq pending;
	struct msg_buf *input;
	/** Counters are useful for finer-grained scheduling. */
	int n_input;
	/**
	 * When pushing messages, keep the staged input size under
	 * this limit (speeds up message delivery and reduces
	 * latency, while still keeping the bus mutex cold enough).
	 */
	int max_input;
	/**
	 * Rather than flushing input into the pipe
	 * whenever a single message or a batch is
	 * complete, do it once per event loop iteration
	 * or when max_input is reached.
	 */
	struct ev_async flush_input;
	/** The event loop of the producer cord. */
	struct ev_loop *producer;
	/**
	 * The cbus endpoint at the destination cord to handle
	 * flushed messages.
	 */
	struct cbus_endpoint *endpoint;
	bool done;
	/**
	 * Triggers to call on flush event, if the input queue
	 * is not empty.
	 */

	struct rlist on_flush;
};

/**
 * Initialize a pipe and connect it to the consumer.
 * Must be called by the producer. The call returns
 * only when the consumer, identified by consumer name,
 * has joined the bus.
 */
void
cpipe_create(struct cpipe *pipe, const char *consumer, struct slab_cache *slabc);

/**
 * Deinitialize a pipe and disconnect it from the consumer.
 * Must be called by producer. Will flash queued messages.
 */
void
cpipe_destroy(struct cpipe *pipe);

/**
 * Set pipe max size of staged push area. The default is infinity.
 * If staged push cap is set, the pushed messages are flushed
 * whenever the area has more messages than the cap, and also once
 * per event loop.
 * Otherwise, the messages flushed once per event loop iteration.
 *
 * @todo: collect bus stats per second and adjust max_input once
 * a second to keep the mutex cold regardless of the message load,
 * while still keeping the latency low if there are few
 * long-to-process messages.
 */
static inline void
cpipe_set_max_input(struct cpipe *pipe, int max_input)
{
	pipe->max_input = max_input;
}

/**
 * Flush all staged messages into the pipe and eventually to the
 * consumer.
 */
static inline void
cpipe_flush_input(struct cpipe *pipe)
{
	assert(loop() == pipe->producer);

	/** Flush may be called with no input. */
	if (pipe->n_input > 0) {
		if (pipe->n_input < pipe->max_input) {
			/*
			 * Not much input, can deliver all
			 * messages at the end of the event loop
			 * iteration.
			 */
			ev_feed_event(pipe->producer,
				      &pipe->flush_input, EV_CUSTOM);
		} else {
			/*
			 * Wow, it's a lot of stuff piled up,
			 * deliver immediately.
			 */
			ev_invoke(pipe->producer,
				  &pipe->flush_input, EV_CUSTOM);
		}
	}
}

static inline void *
pipe_alloc(struct cpipe *pipe, void *data, size_t size)
{
	void *res = ibuf_alloc(&pipe->input->data, size);
	memcpy(res, data, size);
	return res;
}

#define CALL_ALLOC_CLOSE(ARGS...) ARGS )
#define CALL_ALLOC(PIPE) pipe_alloc EMPTY() (PIPE, CALL_ALLOC_CLOSE

/**
 * Push a single message to the pipe input. The message is pushed
 * to a staging area. To be delivered, the input needs to be
 * flushed with cpipe_flush_input().
 */
#define cpipe_push_input(pipe, FUNC, ARGS...)					\
{										\
	assert(loop() == (pipe)->producer);					\
	make_call(CALL_ALLOC(pipe), FUNC, ARGS);				\
	(pipe)->n_input++;							\
	if ((pipe)->n_input >= (pipe)->max_input)				\
		ev_invoke((pipe)->producer, &(pipe)->flush_input, EV_CUSTOM);	\
}

/**
 * Push a single message and ensure it's delivered.
 * A combo of push_input + flush_input for cases when
 * it's not known at all whether there'll be other
 * messages coming up.
 */
#define cpipe_push(pipe, FUNC, ARGS...)						\
{										\
	cpipe_push_input(pipe, FUNC, ARGS);					\
	if ((pipe)->n_input == 1)						\
		ev_feed_event((pipe)->producer, &(pipe)->flush_input,		\
			      EV_CUSTOM);					\
}

void
cbus_init();

/**
 * cbus endpoint
 */
struct cbus_endpoint {
	/**
	 * Endpoint name, used to identify the endpoint when
	 * establishing a route.
	 */
	char name[FIBER_NAME_MAX];
	/** Member of cbus->endpoints */
	struct rlist in_cbus;
	/** The lock around the pipe. */
	pthread_mutex_t mutex;
	/** A queue with incoming messages. */
	struct stailq output;
	/** Consumer cord loop */
	ev_loop *consumer;
	/** Async to notify the consumer */
	ev_async async;
	/** Count of connected pipes */
	uint32_t n_pipes;
	/** Condition for endpoint destroy */
	struct fiber_cond cond;

	struct slab_cache *slabc;
};

/**
 * Fetch incomming messages to output
 */
static inline void
cbus_endpoint_fetch(struct cbus_endpoint *endpoint, struct stailq *output)
{
	tt_pthread_mutex_lock(&endpoint->mutex);
	stailq_concat(output, &endpoint->output);
	tt_pthread_mutex_unlock(&endpoint->mutex);
}

/** Initialize the global singleton bus. */
void
cbus_init();

/** Destroy the global singleton bus. */
void
cbus_free();

/**
 * Connect the cord to cbus as a named reciever.
 * @param name a destination name
 * @param fetch_cb callback to fetch new messages
 * @retval 0 for success
 * @retval 1 if endpoint with given name already registered
 */
int
cbus_endpoint_create(struct cbus_endpoint *endpoint, const char *name,
		     void (*fetch_cb)(ev_loop *, struct ev_watcher *, int), void *fetch_data);

/**
 * One round for message fetch and deliver */
void
cbus_process(struct cbus_endpoint *endpoint);

/**
 * Run the message delivery loop until the current fiber is
 * cancelled.
 */
void
cbus_loop(struct cbus_endpoint *endpoint);

/**
 * Stop the message delivery loop at the destination the pipe
 * is pointing at.
 */
void
cbus_stop_loop(struct cpipe *pipe);

/**
 * Disconnect the cord from cbus.
 * @retval 0 for success
 * @retval 1 if there is connected pipe or unhandled message
 */
int
cbus_endpoint_destroy(struct cbus_endpoint *endpoint,
		      void (*process_cb)(struct cbus_endpoint *));

/**
 * A helper method to invoke a function on the other side of the
 * bus.
 *
 * Creates the relevant messages, pushes them to the callee pipe and
 * blocks the caller until func is executed in the correspondent
 * thread.
 * Detects which cord to invoke a function in based on the current
 * cord value (i.e. finds the respective pipes automatically).
 * Parameter 'data' is passed to the invoked function as context.
 *
 * @return This function itself never fails. It returns 0 if the call
 * was * finished, or -1 if there is a timeout or the caller fiber
 * is canceled.
 * If called function times out or the caller fiber is canceled,
 * then free_cb is invoked to free 'data' or other caller state.
 *
 * If the argument function sets an error in the called cord, this
 * error is safely transferred to the caller cord's diagnostics
 * area.
*/
struct cbus_call_msg;
typedef int (*cbus_call_f)(struct cbus_call_msg *);

struct fiber;
/**
 * The state of a synchronous cross-thread call. Only func and free_cb
 * (if needed) are significant to the caller, other fields are
 * initialized during the call preparation internally.
 */
struct cbus_call_msg
{
	struct cmsg msg;
	struct diag diag;
	cbus_call_f call_f;
	struct cpipe *caller_pipe;
	struct fiber *caller;
	bool complete;
	int rc;
	/** The callback to invoke in the peer thread. */
	cbus_call_f func;
	/**
	 * A callback to free affiliated resources if the call
	 * times out or the caller is canceled.
	 */
	cbus_call_f free_cb;
};

int
cbus_call(struct cpipe *callee, struct cpipe *caller,
	  struct cbus_call_msg *msg,
	  cbus_call_f func, cbus_call_f free_cb, double timeout);

/**
 * Block until all messages queued in a pipe have been processed.
 * Done by submitting a dummy message to the pipe and waiting
 * until it is complete.
 */
void
cbus_flush(struct cpipe *callee, struct cpipe *caller,
	   void (*process_cb)(struct cbus_endpoint *));

/**
 * Create a two-way channel between existing cbus endpoints.
 * Blocks until both pipes are created.
 *
 * @param dest_name       Name of the destination endpoint, i.e.
 *                        the endpoint at a remote cord we are
 *                        connecting to.
 * @param src_name        Name of the source endpoint, i.e.
 *                        the endpoint at the caller's cord.
 * @param[out] dest_pipe  Pipe from the source to the destination
 *                        endpoint.
 * @param[out] src_pipe   Pipe from the destination to the source
 *                        endpoint.
 * @param pair_cb         Callback invoked at the destination right
 *                        after creating the channel to the source.
 *                        May be NULL.
 * @param pair_arg        Argument passed to @pair_cb.
 * @param process_cb      Function invoked to process cbus messages
 *                        at the source endpoint. Pass NULL if there
 *                        is a fiber processing messages at the source.
 */
void
cbus_pair(const char *dest_name, const char *src_name,
	  struct cpipe *dest_pipe, struct cpipe *src_pipe,
	  void (*pair_cb)(void *), void *pair_arg,
	  void (*process_cb)(struct cbus_endpoint *));

/**
 * Destroy a two-way channel between cbus endpoints.
 * Blocks until both pipes are destroyed.
 *
 * Before proceeding to pipe destruction, this function flushes
 * all cbus messages queued at the destination endpoint by sending
 * a message from the source to the destination and back. The
 * caller may specify a callback to invoke when the message is
 * at the destination endpoint. This can be used to notify the
 * destination that the channel is about to be destroyed and so
 * it must stop generating new messages for it.
 *
 * @param dest_pipe       Pipe from the source to the destination
 *                        endpoint.
 * @param src_pipe        Pipe from the destination to the source
 *                        endpoint.
 * @param unpair_cb       Callback invoked at the destination before
 *                        proceeding to pipe destruction (see above).
 *                        May be NULL.
 * @param unpair_arg      Argument passed to @unpair_cb.
 * @param process_cb      Function invoked to process cbus messages
 *                        at the source endpoint. Pass NULL if there
 *                        is a fiber processing messages at the source.
 */
void
cbus_unpair(struct cpipe *dest_pipe, struct cpipe *src_pipe,
	    void (*unpair_cb)(void *), void *unpair_arg,
	    void (*process_cb)(struct cbus_endpoint *));

#if defined(__cplusplus)
} /* extern "C" */
#endif /* defined(__cplusplus) */

#endif /* TARANTOOL_LIB_CORE_CBUS_H_INCLUDED */
