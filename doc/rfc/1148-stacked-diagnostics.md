# Stacked Diagnostics in Tarantool

* **Status**: In progress
* **Start date**: 30-07-2019
* **Authors**: Kirill Shcherbatov @kshcherbatov kshcherbatov@tarantool.org, Konstantin Osipov @kostja kostja@tarantool.org, Georgy Kirichenko @GeorgyKirichenko georgy@tarantool.org, @Totktonada Alexander Turenko alexander.turenko@tarantool.org,Vladislav Shpilevoy @Gerold103 v.shpilevoy@tarantool.org
* **Issues**: [#1148](https://github.com/tarantool/<repository\>/issues/1148)

## Summary
Support stacked diagnostics for Tarantool allows to accumulate all occurred errors during processing a request. This allows to better understand what has happened and handle errors
correspondingly.

## Background and motivation

Tarantool statements must produce diagnostic information that populates the diagnostics area. This is a Standard SQL requirement and other vendors and languages also have such feature.
Diagnostics area stack must contain a diagnostics area for each nested execution context.

### Current Tarantool's error diagnostics
Currently Tarantool has `diag_set()` mechanism to set a diagnostic error.
The last error is exported with `box.error.last()` endpoint.

In total there are few error classes in Tarantool.
```
ClientError
 | LoggedError
 | AccessDeniedError
 | UnsupportedIndexFeature

XlogError
 | XlogGapError
SystemError
 | SocketError
 | OutOfMemory
 | TimedOut

ChannelIsClosed
FiberIsCancelled
LuajitError
IllegalParams
CollationError
SwimError
CryptoError
```

All ClientError codes are exported with their error codes in box.error folder by their NAMEs.

You may use box.error.new endpoint to create a new error instance of predefined type:
```
tarantool> t = box.error.new(box.error.CREATE_SPACE, "myspace", "just I can't")
tarantool> t:unpack()
---
- type: ClientError
  code: 9
  message: 'Failed to create space ''myspace'': just I can''t'
  trace:
  - file: '[string "t = box.error.new(box.error.CREATE_SPACE, "my..."]'
    line: 1
```

User is allowed to define own errors with any code (including system errors) with
```
box.error.new{code = user_code, reason = user_error_msg}
```
where `user_code` must be greater that the biggest registered system error, say `10000`.

Error cdata object has `:unpack()`, `:raise()`, `:match(...)`, `:__serialize()` methods and uniform `.type`, `.message` and `.trace` fields.


## Proposal
In some cases a diagnostic information must be more complicated.
For example, when persistent Lua function referenced by functional index has a bug in it's definition, Lua handler sets an diag message. Then functional index extractor code setups an own, more specialized error.

We need to introduce instruments to deal with it both in C and Lua API. Let's overview them one-by-one:

### C API
The existent `diag_set()` method should be kept as is: it should replace the last error in diagnostic area with a new one.

Let's introduce a new method `diag_add()` that keeps an existent error message in diagnostic area (if any) and sets it as a reason error for a recently-constructed error object.

We also need a way to reset last errors set. Let's introduce `diag_svp(diag) -> SVP`,
`diag_rollback_to_svp(diag, SVP)`. The implementation proposal for SVP structure is given below:

```
                      truncate
      xxxxxxxxxxxxxxxxxxxxxxxx SVP
DIAG: +-------+               |
      | err   |               |
      +-------+----->+-------+ here
      third    prev  | err   |
                     +-------+------->+-------+
                      second  prev    | err   |
                                      +-------+
                                       first
```

The diag_structure already has `struct error *next` pointer;

Let's introduce a similar pointer in an error structure to organize a reason list:
```
struct error {
   ...
   struct error *prev;
};
```

To implement a SVP object, we may return a last-set error pointer (diag::last):
```
diag_set(diag, FIRST)
struct error *SVP = diag_svp(diag)
       /**********************************\
       * return diag->last                *
       \**********************************/

diag_add(diag, SECOND)
diag_add(diag, THIRD)

diag_rollback_to_svp(diag, svp)
       /***********************************\
       * struct error *tmp = diag->last;   *
       * while (tmp != svp) {              *
       *    diag->last = tmp->prev;        *
       *    error_unref(tmp);              *
       *    tmp = diag->last;              *
       * }                                 *
       \***********************************/
```

### Lua API
Tarantool returns a last-set (diag::last) error object as `cdata` from central diagnostic
area to Lua in case of error. User shouldn't have an ability to modify it
(because he actually needn't and because it is dangerous - he doesn't own this object),
but it need an ability to inspect a collected diagnostics information.

Let's extend the `box.error` API with a couple of new functions:
1) `prev` that allows to get a reason of given error:

```
-- Return a reason error object for given error
-- (when exists, nil otherwise)
box.error.prev(error)
```
### Binary protocol
Currently errors are sent as `(IPROTO_ERROR | errcode)` messages with an error's message string payload.

We must to design a backward-compatible error transfer specification. Imagine a net.box old-new Tarantool connection, same as existing connector: they mustn't broken.

Let's send error messages as before, but add a field with `IPROTO_ERROR_LIST` key in addition to existing `IPROTO_ERROR (0x31)` to a body map.

The error's payload would consists of backward-compatible last recent error in the beginning, and a list of reason's serialized errors then. Error packets are rare and not normal, so we may not optimize them too much in terms of payload's size.

```
header: [IPROTO_ERROR_LIST | IPROTO_ERROR |  error code]
pyload: MSGPACK_ERR_MSG_STR..[{code = reason1Code, msg = reason1Msg},..
                              {code = reasonNCode, msg = reasonNMsg}]
```

### SQL Warnings
SQL Standard defines ["WARNINGS"](https://docs.microsoft.com/en-us/sql/t-sql/statements/set-ansi-warnings-transact-sql?view=sql-server-2017) term:
- When set to ON, if null values appear in aggregate functions, such as SUM, AVG, MAX, MIN, STDEV, STDEVP, VAR, VARP, or COUNT, a warning message is generated. When set to OFF, no warning is issued.
- When set to ON, the divide-by-zero and arithmetic overflow errors cause the statement to be rolled back and an error message is generated. When set to OFF, the divide-by-zero and arithmetic overflow errors cause null values to be returned. The behavior in which a divide-by-zero or arithmetic overflow error causes null values to be returned occurs if an INSERT or UPDATE is tried on a character, Unicode, or binary column in which the length of a new value exceeds the maximum size of the column. If SET ANSI_WARNINGS is ON, the INSERT or UPDATE is canceled as specified by the ISO standard. Trailing blanks are ignored for character columns and trailing nulls are ignored for binary columns. When OFF, data is truncated to the size of the column and the statement succeeds.

According to the current convention, all Tarantool's function use "return not null error code on error" convention. Iff the global diagnostic area is valid (actually, Unix errno uses the same semantics). So we need an additional `Parser`/`VDBE` marker to indicate that the warning diagnostics
is set. Say, in addition to `bool is_aborted` field we may introduce and additional field `bool is_warning_set`.

To avoid a mess between SQL warnings and real errors let's better introduce an
"diagnostics"-semantics area in Parser/VDBE classes where all warnings are
organized in a list. Internal warning representation needn't follow error
specification and may use other representation when it is reasonable.

### Plans
Ticket [#4398](https://github.com/tarantool/tarantool/issues/4398) "Expose box.error to be used by applications" requires to design a system to construct error objects with reasons in Lua.
As it was described above, user shouldn't modify an original error, because he didn't construct it
and doesn't hold it's life cycle. However he is able to take a reference on that error. Thus instead of
Go-like `wrap()` semantics we plan to extend a `box.error.new()` constructor to
support `reason` object-typed overload. In such case a new error object would be constructed with
 a given reason reference.
