#include "box/error.h"
#include "diag.h"
#include "errcode.h"
#include "memory.h"
#include "fiber.h"
#include "unit.h"
#include "trivia/util.h"

static struct fiber_attr default_attr;

static int
noop_f(va_list ap)
{
	return 0;
}

static int
cancel_f(va_list ap)
{
	fiber_set_cancellable(true);
	while (true) {
		fiber_sleep(0.001);
		fiber_testcancel();
	}
	return 0;
}

static int
exception_f(va_list ap)
{
	tnt_raise(OutOfMemory, 42, "allocator", "exception");
	return 0;
}

static int
no_exception_f(va_list ap)
{
	try {
		tnt_raise(OutOfMemory, 42, "allocator", "exception");
	} catch (Exception *e) {
		;
	}
	return 0;
}

static int
cancel_dead_f(va_list ap)
{
	note("cancel dead has started");
	fiber_set_cancellable(true);
	tnt_raise(OutOfMemory, 42, "allocator", "exception");
	return 0;
}

static size_t stack_expand_limit;

static void NOINLINE
stack_expand(void *ptr)
{
	char buf[2048];
	memset(buf, 0x45, 2048);
	ptrdiff_t stack_diff = (buf - (char *)ptr);
	stack_diff = stack_diff >= 0 ? stack_diff : -stack_diff;
	if (stack_diff < (ptrdiff_t)stack_expand_limit)
		stack_expand(ptr);
}

static int
test_stack_f(va_list ap)
{
	char s;
	stack_expand(&s);
	return 0;
}

static void
fiber_join_test()
{
	header();

	struct fiber *fiber = fiber_new_xc("join", noop_f);
	fiber_set_joinable(fiber, true);
	fiber_wakeup(fiber);
	fiber_join(fiber);

	fiber = fiber_new_xc("cancel", cancel_f);
	fiber_set_joinable(fiber, true);
	fiber_wakeup(fiber);
	fiber_sleep(0);
	fiber_cancel(fiber);
	fiber_join(fiber);

	fiber = fiber_new_xc("exception", exception_f);
	fiber_set_joinable(fiber, true);
	fiber_wakeup(fiber);
	try {
		if (fiber_join(fiber) != 0)
			diag_raise();
		fail("exception not raised", "");
	} catch (Exception *e) {
		note("exception propagated");
	}

	fputs("#gh-1238: log uncaught errors\n", stderr);
	fiber = fiber_new_xc("exception", exception_f);
	fiber_wakeup(fiber);

	/*
	 * A fiber which is using exception should not
	 * push them up the stack.
	 */
	fiber = fiber_new_xc("no_exception", no_exception_f);
	fiber_set_joinable(fiber, true);
	fiber_wakeup(fiber);
	fiber_join(fiber);
	/*
	 * Trying to cancel a dead joinable cancellable fiber lead to
	 * a crash, because cancel would try to schedule it.
	 */
	fiber = fiber_new_xc("cancel_dead", cancel_dead_f);
	fiber_set_joinable(fiber, true);
	fiber_wakeup(fiber);
	/** Let the fiber schedule */
	fiber_wakeup(fiber());
	fiber_yield();
	note("by this time the fiber should be dead already");
	fiber_cancel(fiber);
	fiber_join(fiber);

	footer();
}

void
fiber_stack_test()
{
	header();

	struct fiber *fiber;
	struct fiber_attr *fiber_attr;

	/*
	 * Test a fiber with the default stack size.
	 */
	stack_expand_limit = default_attr.stack_size * 3 / 4;
	fiber = fiber_new_xc("test_stack", test_stack_f);
	fiber_wakeup(fiber);
	fiber_sleep(0);
	note("normal-stack fiber not crashed");

	/*
	 * Test a fiber with a custom stack size.
	 */
	fiber_attr = fiber_attr_new();
	fiber_attr_setstacksize(fiber_attr, default_attr.stack_size * 2);
	stack_expand_limit = default_attr.stack_size * 3 / 2;
	fiber = fiber_new_ex("test_stack", fiber_attr, test_stack_f);
	fiber_attr_delete(fiber_attr);
	if (fiber == NULL)
		diag_raise();
	fiber_wakeup(fiber);
	fiber_sleep(0);
	note("big-stack fiber not crashed");

	footer();
}

void
fiber_name_test()
{
	header();
	note("name of a new fiber: %s.\n", fiber_name(fiber()));

	fiber_set_name(fiber(), "Horace");

	note("set new fiber name: %s.\n", fiber_name(fiber()));

	const char *long_name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"\
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

	fiber_set_name(fiber(), long_name);

	note("fiber name is truncated: %s.\n", fiber_name(fiber()));
	footer();
}

void
diag_test()
{
	header();
	plan(28);

	note("constract e1 in global diag, share with local diag");
	diag_set(ClientError, ER_PROC_LUA, "runtime error");
	struct diag local_diag;
	diag_create(&local_diag);
	/* Copy a last error to the local diagnostics area. */
	diag_add_error(&local_diag, diag_last_error(diag_get()));

	struct error *e1 = diag_last_error(&local_diag);
	is(e1, diag_last_error(diag_get()),
	   "e1 is an error shared between local and global diag");
	is(e1->refs, 2, "e1::refs: global+local diag");

	note("append e2 to global diag, usr error_ref(e2)");
	diag_add(ClientError, ER_LOAD_FUNCTION, "test", "internal error");
	struct error *e2 = diag_last_error(diag_get());
	error_ref(e2);

	is(e2->reason, e1, "e2::reason == e1");
	is(e1->reason, NULL, "e1::reason == NULL");
	is(e1->refs, 2, "e1::refs: e2 + global diag");
	is(e2->refs, 2, "e2::refs: usr + global diag");

	note("diag_clean global diag");
	diag_clear(diag_get());
	is(e1->refs, 2, "e1::refs: e2 + local");
	is(e2->refs, 1, "e2::refs: usr");
	note("error_unref(e2) -> e2 destruction");
	error_unref(e2);
	e2 = NULL;
	is(e1->refs, 1, "e1::refs: local diag");

	/* Test rollback to SVP. */
	note("diag_move(from = local, to = global): move e1 to global");
	diag_move(&local_diag, diag_get());
	is(diag_is_empty(&local_diag), true, "local diag is empty");
	is(diag_is_empty(diag_get()), false, "global diag is not empty");
	is(diag_last_error(diag_get()), e1, "global diag::last == e1");

	note("svp = diag_svp(global), i.e. 'diag_last(global) = e1' state");
	struct error *svp = diag_svp(diag_get());
	fail_if(svp != e1);
	fail_if(diag_last_error(diag_get()) != e1);
	fail_if(e1->reason != NULL);
	note("append e3, e4 to global diag");
	note("usr error_ref(e1), error_ref(e3), error_ref(e4)");
	diag_add(ClientError, ER_LOAD_FUNCTION, "test", "internal error");
	struct error *e3 = diag_last_error(diag_get());
	error_ref(e3);
	diag_add(ClientError, ER_FUNC_INDEX_FUNC, "func_idx", "space",
		 "everything is bad");
	struct error *e4 = diag_last_error(diag_get());
	error_ref(e4);
	is(e1->refs, 1, "e1::refs: e3");
	is(e3->refs, 2, "e3::refs: usr + e4");
	is(e4->refs, 2, "e4::refs: usr + global diag");
	is(e4->reason, e3, "e4::reason == e3");
	is(e3->reason, e1, "e3::reason == e1");
	note("diag_rollback_to_svp(global, svp)");
	/*
	 * Before rollback there is a sequence
	 * DIAG[e4]->e3->e1->NULL;
	 * After rollback there would be DIAG[e1]->NULL and
	 * a sequence e4->e3->e1->NULL.
	 */
	diag_rollback_to_svp(diag_get(), svp);
	is(e1->refs, 2, "e1::refs: e3 + global diag %d/%d", e1->refs, 2);
	is(e3->refs, 2, "e3::refs: usr + e4");
	is(e4->refs, 1, "e4::refs: usr");
	is(diag_last_error(diag_get()), e1, "diag_last(global) = e1");
	/* Rollback doesn't change error objects itself. */
	is(e4->reason, e3, "e4::reason == e3");
	is(e3->reason, e1, "e3::reason == e1");
	error_unref(e4);
	e4 = NULL;
	is(e3->refs, 1, "e3::refs: usr");
	error_unref(e3);
	e3 = NULL;

	note("ensure that sequential rollback is no-op");
	diag_rollback_to_svp(diag_get(), svp);
	is(e1->refs, 1, "e1::refs: global diag");

	diag_clear(diag_get());
	/*
	 *           usr ref    SVP
	 *   DEL!       |        |
	 *  DIAG[#5] -> #4 -> DIAG'[#3] -> #2 -> #1
	 *
	 * 1) diag_rollback_to_svp
	 * del   <-----------<------>
	 */
	note("test partial list destruction on rollback");
	diag_add(ClientError, ER_PROC_LUA, "#1");
	struct error *er1 = diag_last_error(diag_get());
	diag_add(ClientError, ER_PROC_LUA, "#2");
	svp = diag_svp(diag_get());
	diag_add(ClientError, ER_PROC_LUA, "#3");
	struct error *er3 = diag_last_error(diag_get());
	diag_add(ClientError, ER_PROC_LUA, "#4");
	struct error *er4 = diag_last_error(diag_get());
	error_ref(er4);
	diag_add(ClientError, ER_PROC_LUA, "#5");
	is(er4->refs, 2, "er4:refs: usr + er5 %d/%d", er4->refs, 2);

	diag_rollback_to_svp(diag_get(), svp);
	note("rollback to svp(er2) -> e5:refs == 0, destruction");
	is(er4->reason, er3, "er4->reason == er3");
	is(er3->refs, 1, "er3:refs: er4");
	is(er3->reason, svp, "er3->reason == svp");
	is(svp->refs, 2, "svp->refs: global diag + er3");
	is(svp->reason, er1, "svp->reason == er1");
	is(er1->refs, 1, "er1->refs: err2");

	/*
	 * usr ref       SVP
	 *  |             |
	 * #4 -> #3 -> DIAG'[#2] -> #1
	 *                 |
	 * DIAG[#7] -> #6 -/
	 *   |
	 * usr ref
	 */
	note("multiple error sequences after rollback");
	diag_add(ClientError, ER_PROC_LUA, "#6");
	diag_add(ClientError, ER_PROC_LUA, "#7");
	struct error *er7 = diag_last_error(diag_get());
	error_ref(er7);
	is(er4->refs, 1, "er4->refs: usr");
	is(er7->refs, 2, "er7->refs: global diag + usr");
	is(svp->refs, 2, "svp->refs: er3 + er6");
	is(svp->reason->refs, 1, "svp->reason->refs: svp");
	diag_rollback_to_svp(diag_get(), svp);
	is(er4->refs, 1, "er4->refs: usr");
	is(er7->refs, 1, "er7->refs: usr");
	is(svp->refs, 3, "svp->refs: global diag + er3 + er6");
	is(svp->reason->refs, 1, "svp->reason->refs: svp");
	diag_clear(diag_get());
	is(svp->refs, 2, "svp->refs: er3 + er6");
	is(svp->reason->refs, 1, "svp->reason->refs: svp");
	error_unref(er4);
	is(svp->refs, 1, "svp->refs: er6");
	is(svp->reason->refs, 1, "svp->reason->refs: svp");
	error_unref(er7);

	footer();
}

static int
main_f(va_list ap)
{
	fiber_name_test();
	fiber_join_test();
	fiber_stack_test();
	diag_test();
	ev_break(loop(), EVBREAK_ALL);
	return 0;
}

int main()
{
	memory_init();
	fiber_init(fiber_cxx_invoke);
	fiber_attr_create(&default_attr);
	struct fiber *main = fiber_new_xc("main", main_f);
	fiber_wakeup(main);
	ev_run(loop(), 0);
	fiber_free();
	memory_free();
	return 0;
}
