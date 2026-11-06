# tests/test_guard_logging_payloads.py
"""Guard: no log call in the integration passes raw payload, token or key bytes.

`AGENTS.md` section 5 forbids raw API payloads, tokens and key material in
log records at every level. The value that leaks is not an identifier but a
piece of the wire: a hex dump of a server response, the first bytes of an
undecodable protobuf, the prefix of a shared secret. This guard watches that
class; `test_guard_logging_identifiers.py` watches class (c) identifiers and
the two do not overlap (address-like leaves there, payload-like leaves here).

Eight shapes are recognised inside a log call's arguments:

  (A) a `.hex()` call, whatever the receiver is called;
  (B) a slice (`x[:n]`, `x[a:b]`) whose name chain carries a payload-like
      name (`hex`, `response`, `payload`, `raw`, `secret`, `identity_key`,
      `_bytes`, `body`, `blob`, `token`);
  (C) a serialised protobuf message (`MessageToJson(...)`,
      `MessageToDict(...)`, `MessageToString(...)`, `.SerializeToString()`),
      which dumps every field of a wire message including credentials;
  (D) a name bound in the same function from an HTTP response body
      (`x = await resp.text()`, `resp.read()`, `resp.json()`), whatever the
      name is called and whether or not it is sliced: `text[:400]` is the
      raw body under a neutral name. The taint follows assignments: a name
      bound from an expression that references a body name is a body too
      (`snippet = _decode(content)[:512]`), unless the reference sits under
      a log-safe wrapper (`len`, `_describe_body`, `_describe_error_response`),
      through tuple unpacking and `for` targets as well (`for line in
      text.splitlines(): key, _, value = line.partition("=")` taints
      `line`, `key` and `value`). Two more seeds: a call that names a
      library producer of a server mapping (`gpsoauth.exchange_token`,
      `perform_oauth`, also through a nested `_run` that calls one), and
      the text of a message of shape (G) (`device_str = str(device)`);
  (E) an unsliced argument whose own name is a whole body (`hex_response`,
      `response_hex`, `result_hex`, `hex_string`, `response_bytes`,
      `raw_data`, `raw_bytes`, `raw_response`, `payload`, `body`, `blob`,
      `merged_device_data`, `gcm_data`, `fcm_data`), passed whole to `%s`.
      The list is exact names, not the substring pattern of (B):
      `payload_len`, `raw_prefix` or `status_raw` carry a length, a prefix
      or a status, and a substring rule over them reported 31 sites of
      which two were payloads;
  (F) a mapping unpacked into a dict display that is a log argument
      (`{**gcm_data, "token": redacted}`), whatever the mapping is called:
      every key the mapping carries reaches the record, and redacting one
      of them by name leaves the others (the AidLogin pair `android_id`,
      `security_token` next to the token) in full;
  (G) a protobuf message passed whole to the record: bare, under
      `str()`/`repr()` or inside an f-string. A message is a parameter
      annotated with a message type (a name imported from a `_pb2` module,
      the module itself as in `DeviceUpdate_pb2.DevicesList`, or the
      `MessageProto` alias), a name bound from a constructor (`Msg()`,
      `Msg.FromString(raw)`), a bare alias or subscript of a message, or
      the target of a `for` over a message field (`for device in
      getattr(device_list, "deviceMetadata", [])`). `%s` renders the text
      format with every server-supplied field value (`HeartbeatAck` carried
      three stream counters, a `DataMessageStanza` carries the push);
      `_msg_str` names the type and the fields instead.

The EID is allowed at any level, in full or truncated (`AGENTS.md`, class
(a)); the leaves that carry it are listed in `_ROTATING_LEAVES` and excused
by name. One further site logs the first four bytes of an FMDN advertisement
frame (`payload[:4].hex()` in the BLE scanner: frame byte plus three EID
bytes, rotating) and is excused by (path, leaf, format-string prefix), so the
exception covers that one call and not every `payload` line of the module.

Blind spot, stated on purpose: the guard sees only shapes (A) to (I). Shape
(E) knows an exact list of names: a whole payload under any other name,
or wrapped (`str(payload)`, `repr(payload)`, `payload.decode()`, an
f-string), is not reported. Shape (D) follows assignments inside one
function, not across calls: a helper that returns the body it was given
under a new name is trusted unless the name it is bound to is used in a log
call of the same function. Shape (F) sees the dict display in the call
itself: a copy bound first (`shown = {**gcm_data, ...}`) or built by
`dict(gcm_data, token=...)` and then logged is not followed. Shape (G) does
not follow a parameter without annotation, a message reached through an
attribute (`self._last_msg`), or a field bound by assignment
(`code = msg.error.code`, `pid = getattr(msg, "persistent_id")`): a field
is a value, not the message, and whether it is a sub-message is not
knowable from the name. Shape (H) treats an
exception constructor as a sink, because its message is logged by a
caller one hop later: `raise X(f"... {body}")` and the two-step form
`err = X(f"... {body}"); raise err` (any call whose name ends in `Error`,
`Exception` or `Failed`, or that is raised in the same function). Shape (I), Auth package only (its AGENTS.md keeps raw exception text
out of the message): inside a handler whose types are not all defined in
this package (`except Exception as exc`, bare `except`, `except
(OSError, ssl.SSLError) as err`), a log call that carries the exception by name, wrapped
(`_clip(exc)`, `str(exc)`, an f-string) or through an attribute other
than a code (`errno`, `error_kind`, `status`, `status_code`, `code`,
`__class__`), and an exception constructor of shape (H) that does
(`raise Own(str(exc)) from exc`: an own exception constructed from a foreign one
carries the producer's text one hop later); `describe_exception`,
`type`, `isinstance`, `len` and `_describe_aas_error_text` clear their own
arguments. A traceback is a sink of
its own, because its last rendered line is `str(exc)` and a chained cause is
rendered too: under `Auth/` every `exc_info=` value other than `False` or
`None` (a name, `True`, an alias bound outside the handler, a tuple,
`sys.exc_info()`, an own exception raised `from` a foreign one) and every
`logger.exception(...)` is reported under the leaf `exc_info`, whatever the
handler; `exception_origin` names the frame without the text. A parameter
annotated with a broad or foreign exception type (`err: BaseException`,
`err: ClientError`, `err: ssl.SSLError | None`, `*errors: BaseException`, an import alias
resolved to its suffix, a local type alias resolved to its value: the PEP 695
statement, the conventional `X: TypeAlias = ...`, a plain assignment including a
multiple, a tuple, a starred and a nested target and a string value, a generic
alias applied to its arguments, and a type parameter's bound, constraints or PEP
696 default, followed through chains, unions, carriers, subscript heads over
every hop, starred and `Unpack`ed slots and string annotations, wherever in the
module it is defined, except that a type parameter counts only inside its own
function, class or alias, where it shadows an alias of the same name; the
expansion is additive, so the guard reports at least what it reported before,
and it stops fail-closed at 512 members or 64 alias hops) binds the name for the whole function, and so does a name
assigned, annotated or not, from `task.exception()` (the done callback of
`_track_task`), so a callee that
logs a caught exception it did not catch itself is a sink too (found in
`fcm_receiver_ha._classify_registration_exception`, 2026-09-18), and so does
a parameter of a module-local helper that a call of the same module passes a
bound name or its text to (`describe(err)`, `describe(cause=err)`,
`self.classify(entry_id, err)`; positional-only, keyword-only, `*args` and
`**kwargs` included, whatever the parameter's annotation; a bare name reaches
a function, `self.` or `cls.` a method of that name in any class; followed
over any number of hops, recursion and cycles to a module-wide fixpoint, and
past 64 rounds every parameter of every module-local callee is bound,
fail-closed), and so does
a name the function derives from one of those by data flow inside the
same function, followed to a fixpoint: an alias, attribute or subscript
target (`detail = exc`, `self.last = err`, `self.errs["k"] = err`), its
text or a derivation of it (`shown = str(err)`, `msg += str(err)`, `"x " +
str(err)`, `"x %s" % err`, `str(err) or ""`, `str(err).lower()`, an
f-string, `", ".join(...)`, `err.args`, `err.strerror`, `getattr(err,
"msg")`, a conditional, a container display or a built-in that rearranges
one (`list(errors)`, `map(str, errors)`), `errors[0]`, a comprehension, a
container mutated by `append`/`extend`/`add`/`update`, a queue filled
by `put`/`put_nowait`), tuple and list
unpacking matched by position (`err, count = t.exception(), 0` binds `err`
only, starred targets included) or, from a carrying value that is no
display, as a whole (`code, text = err.args`), a `for`,
`async for` or comprehension target whose iterable mentions a bound name
(`for err in errors` with `errors: list[ClientError]`, `enumerate(errors)`,
`errors.items()`, which binds the key too) and a `match` capture (`case
ClientError() as err`, `case ClientError(args=[first])`, or any capture
when the subject mentions a bound name); a handler name seeds the same
flow inside its handler (`except OSError as exc: text = str(exc)` binds
`text` there); `sys.exc_info()` is read like `task.exception()`, bound
or passed directly. Any call named `exception()` or `exc_info()` is
read as the task method or `sys.exc_info()`, `_LOGGER.exception(...)`
included (fail-closed).
A module bound by `from` (`from cryptography import exceptions as ce`)
resolves `ce.InvalidTag` like `import` does. Not seen: `except
builtins.Exception`, the result of a function this file does not define
(`exc = _pick(task)`, `kind = _classify(err)`: one file does not show what
it returns; binding every logged result would report 210 sites in `Auth/`,
measured on 2026-10-22, none of them a producer's text; a module-local
helper whose `return` carries a bound exception, or another such helper's
result (`return pick(task)`), is followed only as far as `_returning_calls`
and `_returned_exception_leaves` reach: assigned whole to a name (`e =
pick(task)`, also behind `await`) and inside the arguments of a log call or
of a raised constructor (`_LOGGER.error("%s", pick(task))`, `raise
OwnError(str(pick(task)))`, also in `str()`, an
f-string or `.args`); every other derivation of that result is not
followed, measured on 2026-10-24: `e = pick(task) or 'none'`, a
conditional, a tuple (`e, n = pick(task), 0`), `d.get(k, pick(task))`,
`msg += str(pick(task))`, a text wrapper into a name (`msg =
str(pick(task))`), `errs.append(pick(task))`, `for e in [pick(task)]`, and
the result passed on to another helper that logs it (`show(pick(task))`)),
a lambda bound to a name and called (`g = lambda:
str(err); log(g())`), a local annotation with a foreign type whose value is
the result of such a function (`last: str | ClientError | None =
_pick(task)`) and a handler name aliased out
of its handler (`except Exception as exc: last_error = exc`): the package
narrows such a name before each of its three log calls (`fcmregister.py`:
one by `isinstance`, two by a `str` assignment in the same branch) and a
flow-insensitive binding would report all three,
`getattr(t, "exception")()`, a lambda default (`lambda e=err:`), an
insertion written as a function rather than a method
(`heapq.heappush(parts, err)`, `bisect.insort(parts, err)`: the receiver
of such a call is the module, so a mutator entry would bind the wrong
name; measured on 2026-09-20, the package has no such site, and the three
`.put(` calls it does have sit in `eid_resolver.py`, outside this guard's
scope), a binding
in one method read in another (`self.last = err` in `m`, logged in `n`:
the flow stays inside one function), a `with … as err` whose manager
this file does not define (what `__enter__` returns is the manager's
business, not the name's; binding every such target would bind 10 targets
in `Auth/`, measured on 2026-10-24: six HTTP responses of `async with
self._session.post(...) as resp`, one `ThreadPoolExecutor` and three files
from `open`; a module-local generator whose `yield` carries a bound
exception is followed as the manager of a `with`, see `_yielding_calls`,
but not when iterated (`for e in gen(task)`), not a class this file
defines with its own `__enter__`, a manager bound to a name before the
`with` (`cm = held(err)`, then `with cm as e`), a manager entered by hand
(`stack.enter_context(held(task))`, `held(task).__enter__()`), or a
`yield from`),
a helper reached otherwise than by its bare name or through `self.`/`cls.`
(another receiver `self._client.h(err)`, an imported helper: this guard
parses one file, `functools.partial`, an alias of a function `g = h; g(err)`,
a lambda, a `getattr` dispatch, a hand-off to a callback or an executor:
`hass.async_add_executor_job(h, err)`, `loop.run_in_executor(None, h,
err)`), an alias
resolved to a symbol without a suffix in a module that cannot be imported
(`from mylib import Result as ClientError`: the symbol's name decides, not
the alias's, and `mylib` cannot be asked),
a module alias whose name ends
in a suffix (`import errorlib as Error`, resolved to the module, a module is
not a type), a type alias bound in *another* module (`from ._typing import
Failure`, then `err: Failure`: this guard parses one file, and a name bound
from a relative import counts as this package's own anyway), an alias whose
value is the result of a call (`type Made = NewType("Made", ClientError)`,
`cast(...)`: the same promise as for the result of a function this file does
not define), the annotation
of a *local* assignment (`def f(): err: A = make()` does not bind `A`; reading
it as a binding would report three sites in `fcmregister.py` today and would
contradict what `_flow_bound_names` promises), and a bare `type` or `Callable`
reached through an alias (`type Kls = type`, then `err: Kls[ClientError]`): the
resolved head goes through the same three-way rule as a head without an alias,
and neither is `Annotated` nor a carrier, so `type[X]` and `Callable[..., X]`
keep carrying a class and not an instance. An alias whose value does not parse
(`type Broken = "!!"`) is no crash and no finding: the alias name stays a
member and the unparsable value adds none. `from __future__ import annotations`
changes none of this: this guard parses source, the future statement does not
change the AST, and the string annotations it produces at runtime are already
covered by the string branch of `_annotation_members`. a `TypeVar` bound to an exception, in the call form
(`T = TypeVar("T", bound=ClientError)`) as much as in the PEP 695 form
(`def f[T](err: T)` without a bound): a name whose value is a call says nothing
about its type, and an unbounded parameter names none. What *is* read is the
bound, the constraints and the PEP 696 default themselves (`def f[T:
ClientError]`, `T: (ClientError, int)`, `type Box[T = ClientError]`): each
promises that the bound value has that type, which is what this shape looks
for, and a default counts beside an argument too (fail-closed). A generic
alias's arguments are bound into its value where it is used (`type Maybe[T] =
T | None` with `Maybe[ClientError]`). They count inside the function or class
that declares them (nested scopes included) or for the alias that uses them,
never for another `T` of the same module, and there they shadow a module alias
of the same name. A conditional alias value
(`X = A if c else B`) is not collected either, `traceback.format_exc()`,
an own exception that subclasses a foreign one (an instance carries the
text it is built with, and that is read at its constructor; the 27
constructions of such classes in the package carry none, measured on
2026-10-22), an own exception built from a foreign one that is neither
raised nor bound to a name that is raised (`return Own(str(err))`,
`fut.set_exception(Own(str(err)))`: the constructor sink is the one of
shape (H)), a note added to an exception (`err.add_note(str(other))`), and
every module outside `Auth/`
(119 such sites counted on 2026-09-17). Reported although harmless, on purpose:
a rebinding to a harmless value (`err = "safe"; log(err)`), a
nested function whose own parameter shadows a bound name (`def inner(err:
str)`), a number or truth value from a method on the text or the container
(`errors.count(x)`, `str(err).startswith("auth")`) and the index of
`enumerate` or `range` stay reported; the guard is fail-closed there, a
false report costs one review line, and none has an instance today. Shape (D) reads plain and annotated assignments, tuple unpacking (starred
included) and `for` targets, not a walrus, `with ... as` or an augmented
assignment, and follows them to a fixpoint within the function. It trusts names: any `.read()`
counts as a body (a file too), and the helpers in `_SAFE_WRAPPERS` are
trusted whatever they return. The fixpoint over-approximates in the other
direction: a scalar derived from a body (`empty = content == b""`) is a
body name too and would be reported if logged; fail-closed, no instance
today. A raw value under a neutral name that was not read from a
response in the same function (`data`, `msg`), a slice whose whole chain is
neutrally named (`page.read()[:16]`), a wrapping call whose arguments hide
the name (`str(response)[:16]`, `bytes(payload)[:16]`: the chain follows the
callee, not the arguments), `binascii.hexlify()`, `base64.b64encode()` or
`repr(bytes)` are not reported; nor is a message passed whole to `%s`
(`str(message)` is its text format). Arguments of a logging wrapper that is
not itself a log call are unchecked, whatever they carry; the two wrappers
the package has, `_log_verbose` and `_log_warn_with_limit`, are treated as
log calls by name, and so is a name bound in the same function from a log
method (`log_fn = _LOGGER.info if first else _LOGGER.warning`). Shape
(C) also reports `len(x.SerializeToString())`, which would log only a
length; the tree binds serialised bytes to a variable first, so that
false positive has no instance today. Section 5 and the
diff review remain the backstop for those; the remaining gap is not
countable. The scan is package-wide and covers all levels, written as an AST
walk rather than a pin on the sites that were found, because the leak
reappears with every new log line.
"""

from __future__ import annotations

import ast
import builtins
import importlib
import re
import types
from collections.abc import Iterator, Mapping
from pathlib import Path

import pytest

from custom_components import googlefindmy

_PACKAGE_ROOT = Path(googlefindmy.__file__).resolve().parent

# Leaf names that carry wire bytes, key material or a token. `hex` covers
# `response_hex`, `result_hex`, `hex_string`; `_bytes` covers `response_bytes`
# and `prefix_bytes`; `raw` covers `raw_encrypted_identity_key`; `token` covers
# the FCM push token. `key` is deliberately absent: `key[1][:8]` in the tree
# is a canonical-id tuple, and the key material sites all match `.hex()`.
_PAYLOAD_NAME = re.compile(
    r"hex|response|payload|raw|secret|identity_key|_bytes|body|blob|token",
    re.IGNORECASE,
)

# Leaves that carry the EID (class (a), allowed in full or truncated at any
# level). Checked against the base leaf of a `.hex()` receiver or a slice, so
# `eid[:8].hex()`, `eid_hex[:8]` and `truncated_eid_hex[:8]` pass while
# `identity_key[:8].hex()` does not.
_ROTATING_LEAVES: frozenset[str] = frozenset({"eid", "eid_hex", "truncated_eid_hex"})

# Reviewed sites that log a value the guard reads as a payload or as
# exception text: (path relative to the package, base leaf, format-string
# prefix). `payload[:4].hex()` in the BLE scanner is the FMDN frame byte plus
# the first three bytes of the EID of an advertisement that resolved to one
# of the user's own trackers; it rotates with the EID (`docs/FMDN.md`).
# `missing_key` in `fcm_refresh_install_token` is `KeyError.args[0]` from
# four literal lookups on the package's own credentials dict (`"fcm"`,
# `"installation"`, `"refresh_token"`, `"fid"`): the key the code asked
# for, not text a producer wrote (shape (I), 2026-09-18). `_err` in
# `Auth/auth_flow.py` is the `ImportError` of the optional selenium import
# at module level; `missing_browser_dependency` quotes it after the install
# hint (`browser_deps.py`). Its text comes from the import system and names
# a module (`No module named 'selenium'`) or, for a broken installation, a
# path inside it; not a server or a user, and the install hint needs it (shape (I) constructor, 2026-10-23). The prefix keys
# the exception to that one call; `test_each_reviewed_site_matches_
# exactly_one_log_call` fails when the site disappears or is copied.
_REVIEWED: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("fmdn_finder/ble_scanner.py", "payload", "BLE scan: resolved"),
        (
            "Auth/firebase_messaging/fcmregister.py",
            "missing_key",
            "Cannot refresh FCM token: missing credentials key",
        ),
        ("Auth/auth_flow.py", "_err", "raise"),
    }
)

# Exact names of values that are a whole payload (shape (E)); an unsliced
# argument with one of these names is the payload itself.
_WHOLE_BODY_NAMES = frozenset(
    {
        "hex_response",
        "response_hex",
        "result_hex",
        "hex_string",
        "response_bytes",
        "raw_data",
        "raw_bytes",
        "raw_response",
        "payload",
        "body",
        "blob",
        "merged_device_data",
        "gcm_data",
        "fcm_data",
    }
)

# Methods that read an HTTP response body (shape (D)); a name bound from one
# of these in the same function is a body, whatever it is called.
_BODY_READERS = frozenset({"text", "read", "json"})

# Helpers whose return value is log-safe even when a body goes in: the
# guard does not descend into their arguments (shape (D)). `len` and `type`
# yield a number or a name; the describers yield a content class.
_SAFE_WRAPPERS = frozenset(
    {
        "len",
        "type",
        "bool",
        "isinstance",
        "_describe_body",
        "_classify_body",
        "_classify_error_body",
        "_classify_error_code",
        "_describe_error_response",
        "classify_gpsoauth_error",
        # Type and key count of a gpsoauth response, never names or values.
        "_summarize_response",
        "_msg_str",
        "_unknown_field_numbers",
        # Protobuf descriptor access: field names, not values.
        "ListFields",
        "HasField",
        "DESCRIPTOR",
    }
)

# Library calls that return a server response as a mapping (shape (D) seed
# next to the HTTP body readers): the gpsoauth exchange functions. A nested
# function that calls one of them is a producer too, so
# `resp = await loop.run_in_executor(None, _run)` is a body binding.
_BODY_PRODUCERS = frozenset({"exchange_token", "perform_oauth", "perform_master_login"})

# Aliases under which a protobuf message travels as a parameter type
# (shape (G)); the concrete message classes come from the `_pb2` imports of
# each module.
_MESSAGE_ALIASES = frozenset({"MessageProto", "RuntimeMessage"})

# Callables that serialise a whole protobuf message (shape (C)).
_SERIALISERS = frozenset(
    {"MessageToJson", "MessageToDict", "MessageToString", "SerializeToString"}
)

_LOG_LEVELS = frozenset(
    {"debug", "info", "warning", "warn", "error", "exception", "critical", "log"}
)


def _chain(node: ast.AST) -> list[str]:
    """Every name on the way from a value down to the name it hangs on.

    `payload["web"]["endpoint"][:48]` -> `["payload"]`; `self.key[:8]` ->
    `["key"]`; `response.content[:100]` -> `["content", "response"]`;
    `resp.read()[:16]` -> `["read", "resp"]`; `eid[:8].hex()` (receiver of
    `.hex()`) -> `["eid"]`. The whole chain is matched against
    `_PAYLOAD_NAME`, so a payload-named object slices as a payload even when
    the last attribute or method has a neutral name.
    """
    names: list[str] = []
    while True:
        if isinstance(node, ast.Subscript):
            node = node.value
        elif isinstance(node, ast.Attribute):
            if node.attr != "hex":
                names.append(node.attr)
            node = node.value
        elif isinstance(node, ast.Call):
            node = node.func
        elif isinstance(node, ast.Name):
            names.append(node.id)
            return names
        else:
            return names


def _base_leaf(node: ast.AST) -> str | None:
    """The outermost name of a chain (used for the rotating and reviewed lists)."""
    chain = _chain(node)
    return chain[0] if chain else None


def _payload_leaves(argument: ast.AST) -> list[tuple[str, str]]:
    """(shape, base leaf) for every payload-shaped node inside one argument."""
    found: list[tuple[str, str]] = []
    for node in ast.walk(argument):
        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else getattr(func, "id", "")
            )
            if name == "hex" and isinstance(func, ast.Attribute):
                found.append(("hex", _base_leaf(func.value) or "?"))
            elif name in _SERIALISERS:
                target = node.args[0] if node.args else getattr(func, "value", func)
                found.append(("serialised", _base_leaf(target) or name))
        elif isinstance(node, ast.Subscript) and isinstance(node.slice, ast.Slice):
            chain = _chain(node.value)
            if any(_PAYLOAD_NAME.search(name) for name in chain):
                found.append(("slice", chain[0]))
    return found


# Logging wrappers of the package: a call to one of these is a log call even
# though the receiver is `self`.
_LOG_WRAPPERS = frozenset({"_log_verbose", "_log_warn_with_limit"})


def _names_in_target(target: ast.AST) -> list[str]:
    """Names bound by an assignment or loop target, unpacking included."""
    if isinstance(target, ast.Name):
        return [target.id]
    if isinstance(target, (ast.Tuple, ast.List)):
        return [name for element in target.elts for name in _names_in_target(element)]
    if isinstance(target, ast.Starred):
        return _names_in_target(target.value)
    return []


def _assignments(function: ast.AST) -> list[tuple[list[str], ast.AST]]:
    """(bound names, value) of every binding in `function`.

    Plain and annotated assignments, tuple unpacking
    (`key, _, value = line.partition("=")`) and `for` targets
    (`for line in text.splitlines()`), whose value is the iterable.
    """
    found: list[tuple[list[str], ast.AST]] = []
    for node in ast.walk(function):
        if isinstance(node, ast.Assign):
            names = [n for target in node.targets for n in _names_in_target(target)]
            found.append((names, node.value))
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            found.append((_names_in_target(node.target), node.value))
        elif isinstance(node, (ast.For, ast.AsyncFor)):
            found.append((_names_in_target(node.target), node.iter))
    return found


def _is_body_read(value: ast.AST, producers: frozenset[str] = frozenset()) -> bool:
    """A body read (`resp.text()`) or a call that names a body producer."""
    if isinstance(value, ast.Await):
        value = value.value
    if not isinstance(value, ast.Call):
        return False
    if isinstance(value.func, ast.Attribute) and value.func.attr in _BODY_READERS:
        return True
    names = {
        node.attr if isinstance(node, ast.Attribute) else node.id
        for node in ast.walk(value)
        if isinstance(node, (ast.Name, ast.Attribute))
    }
    return bool(names & (_BODY_PRODUCERS | producers))


def _producer_names(function: ast.AST) -> frozenset[str]:
    """Nested functions of `function` that call a body producer."""
    names: set[str] = set()
    for node in ast.walk(function):
        if node is function or not isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef)
        ):
            continue
        if any(
            isinstance(inner, ast.Call) and _is_body_read(inner)
            for inner in ast.walk(node)
        ):
            names.add(node.name)
    return frozenset(names)


def _body_names(
    function: ast.AST, messages: frozenset[str] = frozenset()
) -> frozenset[str]:
    """Names bound in `function` from a response-body read (shape (D)).

    Fixpoint over the function's assignments: a name bound from a body read,
    from a body producer, from the text of a message (`str(msg)`, `repr`, an
    f-string; shape (G) turned into text), or from any expression that
    references a body name outside a log-safe wrapper, is a body name. The
    order of assignments in the source does not matter; the loop runs until
    no name is added.
    """
    names: set[str] = set()
    producers = _producer_names(function)
    assignments = _assignments(function)
    changed = True
    while changed:
        changed = False
        for bound, value in assignments:
            if not (
                _is_body_read(value, producers)
                or _body_leaves(value, frozenset(names))
                or _message_text_leaves(value, messages)
            ):
                continue
            for name in bound:
                if name not in names:
                    names.add(name)
                    changed = True
    return frozenset(names)


def _log_aliases(function: ast.AST) -> frozenset[str]:
    """Names bound in `function` from a log method (`log_fn = _LOGGER.info`)."""
    names: set[str] = set()
    for bound, value in _assignments(function):
        if any(
            isinstance(node, ast.Attribute)
            and node.attr in _LOG_LEVELS
            and "LOG" in ast.unparse(node.value).upper()
            for node in ast.walk(value)
        ):
            names.update(bound)
    return frozenset(names)


def _body_leaves(argument: ast.AST, bodies: frozenset[str]) -> list[tuple[str, str]]:
    """(shape, name) for every reference to a body name inside one argument.

    A reference under a log-safe wrapper (`len(text)`, `_describe_body(text)`)
    is not a leak; the walk stops there.
    """
    found: list[tuple[str, str]] = []

    def walk(node: ast.AST) -> None:
        if isinstance(node, ast.Call):
            func = node.func
            name = (
                func.attr
                if isinstance(func, ast.Attribute)
                else getattr(func, "id", "")
            )
            if name in _SAFE_WRAPPERS:
                return
        if isinstance(node, ast.Name) and node.id in bodies:
            found.append(("body", node.id))
        if isinstance(node, ast.IfExp):
            # Only the value branches reach the sink; the condition
            # (`x if body else y`) tests the body without exposing it.
            walk(node.body)
            walk(node.orelse)
            return
        for child in ast.iter_child_nodes(node):
            walk(child)

    walk(argument)
    return found


def _whole_body_leaves(argument: ast.AST) -> list[tuple[str, str]]:
    """(shape, name) when the argument itself is a whole-body name (shape (E))."""
    if isinstance(argument, (ast.Name, ast.Attribute)):
        chain = _chain(argument)
        if chain and chain[0] in _WHOLE_BODY_NAMES:
            return [("whole", chain[0])]
    return []


def _expansion_leaves(argument: ast.AST) -> list[tuple[str, str]]:
    """(shape, name) for every mapping unpacked into a dict display (shape (F)).

    `ast.Dict` marks a `**` entry with a `None` key; the value is the mapping.
    """
    found: list[tuple[str, str]] = []
    for node in ast.walk(argument):
        if isinstance(node, ast.Dict):
            for key, value in zip(node.keys, node.values, strict=True):
                if key is None:
                    found.append(("expansion", _base_leaf(value) or "?"))
    return found


def _message_type_names(tree: ast.AST) -> frozenset[str]:
    """Protobuf message types a module can name.

    `from ..._pb2 import A, B` contributes `A` and `B`; `import x_pb2` and
    `from pkg import x_pb2` contribute the module name, so a qualified
    annotation `x_pb2.Msg` is a message type by its first segment.
    """
    names = set(_MESSAGE_ALIASES)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            if node.module is not None and node.module.endswith("_pb2"):
                names.update(alias.asname or alias.name for alias in node.names)
            else:
                names.update(
                    alias.asname or alias.name
                    for alias in node.names
                    if alias.name.endswith("_pb2")
                )
        elif isinstance(node, ast.Import):
            names.update(
                (alias.asname or alias.name).split(".")[-1]
                for alias in node.names
                if alias.name.endswith("_pb2")
            )
    return frozenset(names)


def _message_params(function: ast.AST, types: frozenset[str]) -> frozenset[str]:
    """Parameters of `function` annotated with a protobuf message type (shape (G)).

    The annotation may be a bare name, a dotted name, a union (`Msg | None`)
    or a subscript (`list[Msg]`, whose `repr` renders every element); the
    last segment of every name in it is matched against `types`. Declared
    imprecision: `type[Msg]` and `Callable[[Msg], ...]` would match too,
    though they carry a class or a callable, not a message; the package has
    no such parameter today (twelve message parameters, all message-typed).
    """
    if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return frozenset()
    args = function.args
    names: set[str] = set()
    for arg in [
        *args.posonlyargs,
        *args.args,
        *args.kwonlyargs,
        *([args.vararg] if args.vararg else []),
        *([args.kwarg] if args.kwarg else []),
    ]:
        if arg.annotation is None:
            continue
        try:
            text = ast.unparse(arg.annotation)
        except RecursionError:
            # A union of 1000 members nests too deep for `ast.unparse` and
            # ended the scan; the names are then read off the nodes, which
            # `ast.walk` visits without recursion. Every segment counts there,
            # not only the first and the last: wider, never narrower.
            text = " ".join(
                node.id
                if isinstance(node, ast.Name)
                else node.attr
                if isinstance(node, ast.Attribute)
                else str(node.value)
                for node in ast.walk(arg.annotation)
                if isinstance(node, (ast.Name, ast.Attribute))
                or (isinstance(node, ast.Constant) and isinstance(node.value, str))
            )
        parts = re.split(r"[^\w.]+", text)
        if any(
            part.split(".")[-1] in types or part.split(".")[0] in types
            for part in parts
            if part
        ):
            names.add(arg.arg)
    return frozenset(names)


def _message_names(function: ast.AST, types: frozenset[str]) -> frozenset[str]:
    """Message-typed names in `function`: parameters (shape (G)) and bindings.

    Fixpoint over the function's bindings: a name bound from a message
    constructor (`Msg()`, `Msg.FromString(raw)`), as a bare alias or subscript
    of a message name (`m = msg`, `first = items[0]`), or as the target of a
    `for` over a message-rooted chain or `getattr(message, ...)` (a repeated
    field yields sub-messages) is a message name too. An attribute or
    `getattr` bound by assignment is a field value, not a message
    (`code = msg.error.code`), and is not followed.
    """
    names: set[str] = set(_message_params(function, types))
    loop_bound = {
        name
        for node in ast.walk(function)
        if isinstance(node, (ast.For, ast.AsyncFor))
        for name in _names_in_target(node.target)
    }
    changed = True
    while changed:
        changed = False
        for bound, value in _assignments(function):
            is_loop = bool(set(bound) & loop_bound)
            if not (
                _is_message_constructor(value, types)
                or _is_message_derivation(value, frozenset(names), is_loop)
            ):
                continue
            for name in bound:
                if name not in names:
                    names.add(name)
                    changed = True
    return frozenset(names)


def _is_message_derivation(
    value: ast.AST, messages: frozenset[str], is_loop: bool
) -> bool:
    """`msg`, `items[0]`, or (as a loop iterable) `msg.field`/`getattr(msg, …)`."""
    if isinstance(value, ast.Await):
        value = value.value
    if isinstance(value, ast.Call):
        func = value.func
        if not (isinstance(func, ast.Name) and func.id == "getattr" and value.args):
            return False
        return is_loop and _root(value.args[0]) in messages
    if isinstance(value, ast.Name):
        return value.id in messages
    if isinstance(value, ast.Subscript) and not isinstance(value.slice, ast.Slice):
        return _root(value.value) in messages
    if isinstance(value, ast.Attribute):
        return is_loop and _root(value) in messages
    return False


def _root(node: ast.AST) -> str | None:
    """The name a chain hangs on (`device_list` for `device_list.meta[0]`)."""
    chain = _chain(node)
    return chain[-1] if chain else None


def _is_message_constructor(value: ast.AST, types: frozenset[str]) -> bool:
    """`Msg()` or `Msg.FromString(...)` for a message type of the module."""
    if isinstance(value, ast.Await):
        value = value.value
    if not isinstance(value, ast.Call):
        return False
    return any(name in types for name in _chain(value.func))


def _message_text_leaves(
    value: ast.AST, messages: frozenset[str]
) -> list[tuple[str, str]]:
    """Text conversions of a message anywhere inside `value`.

    `str(msg)`, `repr(msg)` or an f-string interpolating it; a bare reference
    (`helper(msg)`, `msg.field`) is not text and is not reported here.
    """
    found: list[tuple[str, str]] = []
    for node in ast.walk(value):
        if isinstance(node, (ast.Call, ast.JoinedStr)):
            found.extend(_message_leaves(node, messages))
    return found


def _message_leaves(
    argument: ast.AST, messages: frozenset[str]
) -> list[tuple[str, str]]:
    """(shape, name) when a message parameter is passed whole (shape (G)).

    Whole means bare, wrapped in `str()`/`repr()`, or interpolated into an
    f-string; `_msg_str(msg)`, `type(msg).__name__` and field access are not
    the message.
    """
    node = argument
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id in {"str", "repr"}
        and len(node.args) == 1
    ):
        node = node.args[0]
    if isinstance(node, ast.Name) and node.id in messages:
        return [("message", node.id)]
    if isinstance(node, ast.JoinedStr):
        return [
            ("message", value.value.id)
            for value in node.values
            if isinstance(value, ast.FormattedValue)
            and isinstance(value.value, ast.Name)
            and value.value.id in messages
        ]
    return []


def _is_log_call(node: ast.Call, aliases: frozenset[str] = frozenset()) -> bool:
    func = node.func
    if isinstance(func, ast.Name):
        return func.id in aliases
    if not isinstance(func, ast.Attribute):
        return False
    if func.attr in _LOG_WRAPPERS:
        return True
    if func.attr not in _LOG_LEVELS:
        return False
    receiver = ast.unparse(func.value)
    return "LOG" in receiver.upper()


def _is_reviewed(
    reviewed: frozenset[tuple[str, str, str]], relative: str, leaf: str, fmt: str
) -> bool:
    # An empty prefix would excuse every line of the module; the exception is
    # meant to name one site, so it must carry a prefix.
    assert all(prefix for _path, _name, prefix in reviewed), (
        "empty prefix in reviewed list"
    )
    return any(
        relative == path and leaf == name and fmt.startswith(prefix)
        for path, name, prefix in reviewed
    )


def scan(
    root: Path = _PACKAGE_ROOT,
    reviewed: frozenset[tuple[str, str, str]] = _REVIEWED,
    rotating: frozenset[str] = _ROTATING_LEAVES,
) -> tuple[list[tuple[str, int, str, str]], int]:
    """Return ((path, line, leaf, format string) offenders, scanned calls).

    Offenders are deduplicated per log call and leaf: a ternary with two
    `.hex()` nodes on the same leaf is one finding, not two.
    """
    offenders: list[tuple[str, int, str, str]] = []
    scanned = 0
    for path in sorted(root.rglob("*.py")):
        if path.name.endswith("_pb2.py") or "vendor" in path.parts:
            continue
        relative = path.relative_to(root).as_posix()
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        found, count = _scan_tree(tree, relative, reviewed, rotating)
        offenders.extend(found)
        scanned += count
    return offenders, scanned


_EXCEPTION_SUFFIXES = ("Error", "Exception", "Failed")
# Every built-in `BaseException` subclass, including the ones without a
# conventional suffix (`ExceptionGroup`, `SystemExit`, `KeyboardInterrupt`,
# `StopIteration`, `GeneratorExit`, the warnings); read from the interpreter
# instead of a hand-kept list, so a new built-in is covered on upgrade.
_BUILTIN_EXCEPTIONS = frozenset(
    name
    for name, obj in vars(builtins).items()
    if isinstance(obj, type) and issubclass(obj, BaseException)
)

# Shape (I): a producer exception logged by name inside a broad handler.
# Scope is the whole Auth package, whose AGENTS.md keeps raw exception text
# out of the message; `fcm_receiver_ha.py` joined the scope on 2026-09-18
# (its 40 sites were rewritten, no file is excused).
_SHAPE_I_ROOT = "Auth/"
_BROAD_EXCEPTIONS = frozenset({"Exception", "BaseException"})
# Wrappers that reduce an exception to a type, a kind or a count. `len` gives a
# count; `_describe_aas_error_text` (`Auth/token_retrieval.py`) names the
# markers of a gpsoauth error text and its length, never the text; both stand
# in exception constructors there (measured on 2026-10-22, AP4 of
# PLAN_GFMY_WAECHTER_LOGGING_LUECKEN: three sites, all harmless).
_SAFE_EXCEPTION_WRAPPERS = frozenset(
    {
        "describe_exception",
        "exception_origin",
        "type",
        "isinstance",
        "len",
        "_describe_aas_error_text",
    }
)
# Calls whose result renders the exception text: binding their result binds
# the name (`shown = str(err)`, `text = ", ".join(str(e) for e in errors)`);
# `_clip` is the package's own truncation.
_TEXT_WRAPPERS = frozenset(
    {
        "str",
        "repr",
        "ascii",
        "format",
        "join",
        "_clip",
        "format_exception",
        "format_exception_only",
    }
)
# Built-ins and stdlib containers that hand their arguments back, rearranged:
# `list(errors)`, `next(iter(errors))`, `map(str, errors)`, `dict(err=err)`,
# `dict.fromkeys(keys, err)`, `deque([err])`; `len()` is not one.
_TRANSPARENT_CALLS = frozenset(
    {
        "list",
        "tuple",
        "set",
        "frozenset",
        "sorted",
        "reversed",
        "iter",
        "next",
        "map",
        "filter",
        "zip",
        "enumerate",
        "dict",
        "min",
        "max",
        "vars",
        "fromkeys",
        "deque",
        "OrderedDict",
        "Counter",
        "defaultdict",
        "ChainMap",
    }
)
# Methods that put an argument into their receiver (`parts.append(str(err))`).
_MUTATORS = frozenset(
    {
        "append",
        "extend",
        "insert",
        "add",
        "update",
        "setdefault",
        "appendleft",
        "extendleft",
        "put",
        "put_nowait",
        "__setitem__",
        "__iadd__",
        "__ior__",
    }
)
# Methods whose default argument comes back as the result (`d.get(k, err)`).
_DEFAULT_METHODS = frozenset({"get", "pop", "setdefault"})

# Attributes of an exception that are a code, not its text.
_SAFE_EXCEPTION_ATTRS = frozenset(
    {"errno", "error_kind", "status", "status_code", "code", "__class__"}
)
# `status_code`: `fcm_receiver_ha._raise_if_fatal_client_error` reads
# `getattr(err, "status_code", None)` and logs the number (2026-09-18).


def _own_type_names(tree: ast.AST) -> frozenset[str]:
    """Exception types this module defines or imports from this package."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            names.add(node.name)
        elif isinstance(node, ast.ImportFrom) and (
            node.level >= 1
            or (node.module or "").startswith("custom_components.googlefindmy")
        ):
            names.update(alias.asname or alias.name for alias in node.names)
    return frozenset(names)


def _is_foreign_handler(handler: ast.ExceptHandler, own: frozenset[str]) -> bool:
    """`except:`, a broad type, or any type not defined in this package.

    Fail-closed: a builtin (`ValueError`) or a library type (`ssl.SSLError`,
    `ECEException`) carries text this package did not write.
    """
    kind = handler.type
    if kind is None:
        return True
    types = list(kind.elts) if isinstance(kind, ast.Tuple) else [kind]
    for node in types:
        chain = _chain(node)
        name = chain[0] if chain else ""
        if name in _BROAD_EXCEPTIONS or name not in own:
            return True
    return False


def _imported_exception_types(tree: ast.AST) -> frozenset[str]:
    """Local names bound by `from x import Y [as Z]` that resolve to exception classes.

    The naming convention (`Error`/`Exception`/`Failed`) and the built-in
    list do not cover a third-party type such as
    `cryptography.exceptions.InvalidTag` (imported in `fcm_receiver_ha.py`);
    the module is importable in the test environment, so the class itself
    is asked. A module that cannot be imported, or a name that is not a
    class, contributes nothing and the convention decides (fail-open here is
    bounded: the convention still applies).
    """
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom) or node.level or not node.module:
            continue
        if node.module.startswith("custom_components.googlefindmy"):
            continue
        try:
            module = importlib.import_module(node.module)
        except Exception:  # noqa: BLE001 - optional or absent dependency
            continue
        for alias in node.names:
            try:
                obj = getattr(module, alias.name, None)
            except Exception:  # noqa: BLE001 - a module `__getattr__` may raise anything
                continue
            if isinstance(obj, type) and issubclass(obj, BaseException):
                names.add(alias.asname or alias.name)
    return frozenset(names)


def _module_bindings(tree: ast.AST) -> dict[str, str]:
    """Local name -> module path for `import a.b` (`a`), `import a.b as c` (`c`)
    and `from a import b [as c]` when `b` is a module (`c` or `b`).

    `from cryptography import exceptions as ce` binds a module, not a class:
    `_imported_exception_types` skips it (the object is no type), so
    `err: ce.InvalidTag` has to be resolved through this table. A `from`
    import whose module cannot be imported, or whose name is not a module,
    contributes nothing (the naming convention decides, as everywhere else).
    """
    bindings: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    bindings[alias.asname] = alias.name
                else:
                    bindings[alias.name.split(".")[0]] = alias.name.split(".")[0]
        elif (
            isinstance(node, ast.ImportFrom)
            and not node.level
            and node.module
            and not node.module.startswith("custom_components.googlefindmy")
        ):
            try:
                parent = importlib.import_module(node.module)
            except Exception:  # noqa: BLE001 - optional or absent dependency
                continue
            for alias in node.names:
                try:
                    member = getattr(parent, alias.name, None)
                except Exception:  # noqa: BLE001 - a module `__getattr__` may raise anything
                    continue
                if isinstance(member, types.ModuleType):
                    bindings[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return bindings


def _is_qualified_exception(annotation: ast.AST, modules: dict[str, str]) -> bool:
    """`err: cryptography.exceptions.InvalidTag` or `err: ce.InvalidTag` resolved as a class.

    The first segment is looked up in the `import` table, the module is
    imported and the remaining segments are walked with `getattr`; the
    result must be a `BaseException` subclass. Anything that cannot be
    imported or resolved falls back to the naming convention.
    """
    if not isinstance(annotation, ast.Attribute):
        return False
    dotted = ast.unparse(annotation).split(".")
    head = modules.get(dotted[0])
    if head is None:
        return False
    parts = head.split(".") + dotted[1:]
    obj: object = None
    for cut in range(len(parts), 0, -1):
        try:
            obj = importlib.import_module(".".join(parts[:cut]))
        except Exception:  # noqa: BLE001 - not a module at this depth
            continue
        for attr in parts[cut:]:
            obj = getattr(obj, attr, None)
            if obj is None:
                return False
        break
    return isinstance(obj, type) and issubclass(obj, BaseException)


def _import_aliases(tree: ast.AST) -> dict[str, str]:
    """Local name -> imported name for `from x import Y as Z` and `import x.Y as Z`.

    `from aiohttp import ClientError as TransportFailure` hides the suffix
    behind the alias; the annotation check resolves the alias first.
    """
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                if alias.asname:
                    aliases[alias.asname] = alias.name.rsplit(".", 1)[-1]
    return aliases


# A local alias binds a name to an *expression*, so it needs a table of its own.
# Two limits guard the expansion, both set rather than derived (measured over
# the 135 modules of this package: the annotations this guard classifies reach
# one alias hop, and any parameter annotation at most three names on one path). Width, because additive expansion of a doubling chain
# (`type Ai = A(i-1) | A(i-1)`) grows as `3*2^n - 1`. Depth, because every hop
# nests another frame: a linear chain adds one member per hop, so the width limit
# alone would not cut it before 512 hops (measured: 511 hops stay silent at 513
# members, 512 hops hit the limit). The depth limit tightens that cut to 64 hops.
# Measured by bisecting `sys.setrecursionlimit` on a 600-hop chain: with the width
# limit alone the guard still runs at a limit of 521 out of the interpreter's
# default 1000, with the depth limit at 73. So the depth limit is margin, not a
# second kind of protection; saying it prevents a `RecursionError` that the width
# limit would not prevent would overstate it, and so would claiming the width
# limit alone leaves no room.
_ALIAS_MEMBER_LIMIT = 512
_ALIAS_DEPTH_LIMIT = 64
# Appended fail-closed when either limit is reached: a truncated expansion must
# report, not go silent.
_ALIAS_LIMIT_MEMBER = ast.Name(
    id="BaseException", ctx=ast.Load(), lineno=0, col_offset=0
)
# Values a plain assignment may bind as a type: a name, a dotted name, a
# subscript or a union. A call (`TypeVar(...)`, `NewType(...)`) is none of them,
# and neither is a literal: what they return is not knowable from the name.
_ALIAS_VALUE_NODES = (ast.Name, ast.Attribute, ast.Subscript)


def _parse_type_string(text: str) -> ast.expr | None:
    """The expression a string annotation or alias value spells, `None` if none.

    Besides a `SyntaxError`, the parser raises a `MemoryError` when its stack
    overflows (measured on a string of 6000 minus signs) and a `RecursionError`
    on deep nesting; the guard parses every string a module assigns, so none
    of them may end the run.
    """
    try:
        return ast.parse(text, mode="eval").body
    except (SyntaxError, ValueError, MemoryError, RecursionError):
        return None


def _is_type_expression(value: ast.AST) -> bool:
    """`ClientError`, `mod.Err`, `list[Err]`, `A | B` or `"ClientError"`, never a call.

    A string counts when its content parses as one of the others, the same
    reading an annotation string gets (`A = "ClientError"` binds what
    `A: TypeAlias = "ClientError"` binds).
    """
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        parsed = _parse_type_string(value.value)
        if parsed is None:
            return False
        value = parsed
    if isinstance(value, ast.BinOp):
        return isinstance(value.op, ast.BitOr)
    return isinstance(value, _ALIAS_VALUE_NODES)


def _local_type_aliases(
    tree: ast.AST, aliases: dict[str, str]
) -> dict[str, tuple[ast.AST, ...]]:
    """Local name -> the type expressions it is bound to, anywhere in the module.

    The import-alias table maps a name to a name; a local alias binds a name to
    an expression, which that table cannot carry. Four sources, all of them in
    Home-Assistant code: the PEP 695 statement (`type X = ClientError`), kept
    whole when it declares type parameters (`type Maybe[T] = T | None`, bound
    where it is used, see `_apply_generic_alias`), the conventional form (`X: TypeAlias = ClientError`, also through
    `typing.TypeAlias`, `"TypeAlias"` and `from typing import TypeAlias as TA`)
    and a plain assignment whose value is a type expression or a string that
    parses as one (`X = ClientError`, `X = "ClientError"`, `A = B =
    ClientError`, `A, B = ClientError, OSError`, `*A, B = str, ClientError`,
    `(A, B), C = (ClientError, OSError), ValueError`). A function's or a
    class's type parameters are not in this table: `_type_param_scopes` adds
    them to the table of their own scope only. The plain form is not
    an afterthought: of this package's seven PEP 695 aliases none points at an
    exception, and all six aliases that do are plain assignments (measured
    2026-09-20).

    Scope is flattened on purpose: an alias defined in a function or a class
    body counts for the whole module, the same over-approximation
    `_import_aliases` and `_own_type_names` already make, and for the same
    reason the order of definition and use does not matter. A name defined more
    than once (`if TYPE_CHECKING:` next to an `else`) keeps *every* value;
    taking the last would go silent as soon as the harmless branch comes last.

    Not collected, each one a promise the module docstring repeats: a name
    bound in another module (this guard parses one file), the result of a call
    (`NewType(...)`, `TypeVar(...)`, `cast(...)`), the annotation of a local
    assignment (`def f(): err: A = make()` does not bind `A`; reading it would
    report three sites in this package today), an attribute or subscript target
    (`mod.X = ...` binds no local name) and a conditional value (`X = A if c
    else B`: both branches would be members, neither is collected). Four more
    binding forms are silent for the same reason, each measured: a walrus
    (`(X := ClientError)`), an augmented assignment (`X += ClientError`), a
    `for` target (`for X in (ClientError,)`) and `with ... as X`. All four bind
    a runtime value rather than a type, and none has an instance in this
    package. The conventional form is recognised through the import-alias table
    only, so a locally aliased `TypeAlias` (`TAL = TypeAlias`, then `X: TAL =
    ClientError`) is not: resolving that would need the table this function is
    still building.
    """
    local: dict[str, list[ast.AST]] = {}

    def _bind(name: str, value: ast.AST | None) -> None:
        if name and value is not None:
            local.setdefault(name, []).append(value)

    def _bind_target(target: ast.AST, value: ast.AST) -> None:
        # Matched by position like `_unpacked_names`, nested targets included
        # (`(A, B), C = (X, Y), Z`); the starred slot takes the remainder, which
        # is a list at runtime and never a type, and the slots behind it count
        # from the end (`*A, B = str, int, ClientError` binds `B`).
        if isinstance(target, ast.Name):
            if _is_type_expression(value):
                _bind(target.id, value)
            return
        if not isinstance(target, (ast.Tuple, ast.List)):
            return
        if not isinstance(value, (ast.Tuple, ast.List)):
            return
        slots, values = list(target.elts), list(value.elts)
        star = next(
            (i for i, slot in enumerate(slots) if isinstance(slot, ast.Starred)), None
        )
        if star is None:
            pairs = list(zip(slots, values))
        else:
            tail = len(slots) - star - 1
            pairs = list(zip(slots[:star], values[:star]))
            if tail and len(values) >= len(slots) - 1:
                pairs += list(zip(slots[star + 1 :], values[len(values) - tail :]))
        for slot, item in pairs:
            _bind_target(slot, item)

    for node in ast.walk(tree):
        if isinstance(node, ast.TypeAlias):
            if isinstance(node.name, ast.Name):
                # A generic alias is kept whole: its value means something
                # only with its parameters bound, and the arguments that bind
                # them are known at the use site (`_apply_generic_alias`). A
                # function's or class's parameters are scoped by
                # `_type_param_scopes`.
                _bind(
                    node.name.id,
                    _GenericAlias(node) if node.type_params else node.value,
                )
        elif isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.value is not None:
                members = _annotation_members(node.annotation, aliases)
                chain = _chain(members[0]) if len(members) == 1 else []
                head = chain[0] if chain else ""
                if aliases.get(head, head) == "TypeAlias":
                    _bind(node.target.id, node.value)
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                _bind_target(target, node.value)
    return {name: tuple(values) for name, values in local.items()}


class _GenericAlias(ast.AST):
    """A PEP 695 alias with type parameters, kept whole in the alias table.

    Its value means something only with its parameters bound: by the arguments
    at the use site (`Maybe[ClientError]` for `type Maybe[T] = T | None`), else
    by their PEP 696 defaults, and always by their bounds or constraints. The
    table keeps the statement rather than a substituted copy, so the binding
    happens where the arguments are known. `_fields` is empty, so `ast.walk`
    does not enter it.
    """

    _fields = ()

    def __init__(self, node: ast.TypeAlias) -> None:
        super().__init__()
        self.node = node


class _Binding:
    """What a type parameter stands for, with the environment and hops to read it in.

    `seen` belongs to where the node was written: an argument is read with the
    caller's hops, not with the alias head the value added, so `L[L[ClientError]]`
    repeats `L` without being a cycle.
    """

    __slots__ = ("env", "node", "seen")

    def __init__(
        self,
        node: ast.AST,
        env: Mapping[str, tuple[_Binding, ...]],
        seen: frozenset[str],
    ) -> None:
        self.node = node
        self.env = env
        self.seen = seen


_NO_ENV: Mapping[str, tuple[_Binding, ...]] = types.MappingProxyType({})
# Appended in place of a bound type parameter's own name: the name is no type
# (`type Res[V, TError] = V | TError` with `Res[int, str]` names no exception),
# but the member keeps the width count that cuts a doubling chain of defaults.
_PARAM_MEMBER = ast.Name(id="<type parameter>", ctx=ast.Load(), lineno=0, col_offset=0)


def _param_values(param: ast.AST) -> tuple[tuple[ast.AST, ...], ast.AST | None]:
    """A type parameter's bound, read as its members if it is a constraint tuple, and its default.

    `T: (ClientError, int)` constrains `T` to exactly one of the two, the
    promise a union makes, so each member counts like a bound; a bound or a
    default promises that the bound value has that type, which is what shape
    (I) looks for. An unbounded parameter names none.
    """
    bound = getattr(param, "bound", None)
    if bound is None:
        bounds: tuple[ast.AST, ...] = ()
    elif isinstance(bound, ast.Tuple):
        bounds = tuple(bound.elts)
    else:
        bounds = (bound,)
    return bounds, getattr(param, "default_value", None)


def _type_param_bindings(node: ast.AST) -> dict[str, tuple[ast.AST, ...]]:
    """Type parameter -> its bound or constraints and PEP 696 default, `{}` when none."""
    bindings: dict[str, tuple[ast.AST, ...]] = {}
    for param in getattr(node, "type_params", ()):
        name = getattr(param, "name", "")
        bounds, default = _param_values(param)
        values = bounds + ((default,) if default is not None else ())
        if name and values:
            bindings[name] = bindings.get(name, ()) + values
    return bindings


def _distribute_arguments(
    params: list[ast.AST], args: list[ast.AST]
) -> tuple[list[list[ast.AST] | None], list[ast.AST]]:
    """The arguments of each parameter (`None` when none is given) and the surplus.

    Positional; a `TypeVarTuple` takes the middle and the parameters behind it
    count from the end (PEP 646), a sole `ParamSpec` takes every argument (PEP
    612). Surplus arguments are handed back so the caller reads them
    fail-closed: `Id[int, ClientError]` is a type error, not a harmless one.
    """
    if len(params) == 1 and isinstance(params[0], ast.ParamSpec):
        return [list(args) or None], []
    star = next(
        (i for i, param in enumerate(params) if isinstance(param, ast.TypeVarTuple)),
        None,
    )
    given: list[list[ast.AST] | None]
    if star is None:
        given = [[arg] for arg in args[: len(params)]]
        given += [None] * (len(params) - len(given))
        return given, list(args[len(params) :])
    front: list[list[ast.AST] | None] = [[arg] for arg in args[:star]]
    rest = list(args[star:])
    split = max(len(rest) - (len(params) - star - 1), 0)
    back: list[list[ast.AST] | None] = [[arg] for arg in rest[split:]]
    given = front + [None] * (star - len(front)) + [rest[:split] or None] + back
    given += [None] * (len(params) - len(given))
    return given, []


def _apply_generic_alias(
    alias: _GenericAlias,
    args: list[ast.AST],
    caller: Mapping[str, tuple[_Binding, ...]],
    caller_seen: frozenset[str],
    aliases: dict[str, str],
    local: dict[str, tuple[ast.AST, ...]],
    seen: frozenset[str],
    members: list[ast.AST],
) -> None:
    """Append the members of `alias`'s value with its parameters bound.

    Each parameter stands for its argument, read in the caller's environment,
    and for its PEP 696 default and its bound or constraints: a bound promises
    that any argument has that type (`type Box[T: ClientError] = list[T]` with
    `Box[str]` stays reported), and a default counts beside an argument as
    well, the one rule for bound and default this guard keeps (fail-closed). A bound and a default are read
    with the earlier parameters bound and every other one unbound, because a
    default may name only earlier parameters (PEP 696): `type Box[T:
    ClientError, U = T] = list[U]` follows `U` to `T` to `ClientError` in one
    pass, and no order of defaults can loop. The value sees these parameters
    and nothing of the caller: its own `T` is not the `T` of the function
    that uses the alias. The arguments keep the caller's hops (`caller_seen`),
    the value and its bounds and defaults the hops that include the alias
    head (`seen`), so a nested use of the same alias is no cycle. The
    parameters are bound lazily, where the value is walked, so there is no
    copy and no transformer whose recursion a wide union could exhaust.
    """
    params = list(alias.node.type_params)
    names = [getattr(param, "name", "") for param in params]
    env: dict[str, tuple[_Binding, ...]] = dict.fromkeys(names, ())
    given, surplus = _distribute_arguments(params, args)
    for param, name, got in zip(params, names, given):
        bounds, default = _param_values(param)
        scope = types.MappingProxyType(dict(env))
        bindings = [_Binding(bound, scope, seen) for bound in bounds]
        if got is not None:
            bindings += [_Binding(arg, caller, caller_seen) for arg in got]
        if default is not None:
            # Beside an argument too: the default is read as a member wherever
            # it is declared, the rule this guard set for bound and default
            # alike (fail-closed; `Box[str]` with `T = ClientError` reports).
            bindings.append(_Binding(default, scope, seen))
        env[name] = tuple(bindings)
    for arg in surplus:
        _collect_annotation_members(arg, aliases, local, caller_seen, members, caller)
    _collect_annotation_members(alias.node.value, aliases, local, seen, members, env)


def _origin(value: ast.AST) -> ast.AST:
    """The tree node an alias-table value came from."""
    return value.node if isinstance(value, _GenericAlias) else value


def _type_param_scopes(
    tree: ast.AST, local: dict[str, tuple[ast.AST, ...]]
) -> dict[int, dict[str, tuple[ast.AST, ...]]]:
    """`id(node)` -> the alias table as seen inside the functions and classes that declare type parameters.

    A type parameter is scoped to the function or class that declares it,
    nested scopes included, and a class reads its parameters like a function
    (`class K[T: ClientError]` with `def m(self, err: T)` is reported). A
    declared parameter shadows every entry of its name in this alias table
    made outside the declaring node, bounded or not: an enclosing function's
    parameter (`def a[T: ClientError](): def b[T](err: T)` is silent) as much
    as a module alias (`T: TypeAlias = ClientError`, then `def a[T: int]():
    def g(err: T)` is silent). A binding inside the declaring node (`def a[T:
    int](): T = ClientError`) rebinds the name and stays, and inside that node
    the scope is flattened like the table itself: a binding in one nested
    function counts for its siblings too (fail-closed). An import alias is not
    in this table and is not shadowed (`from aiohttp import ClientError as
    Failure`, then `def g[Failure: int](err: Failure)` stays reported, as it
    was before type parameters were read). Nodes inside no declaring scope
    are absent and read `local` itself.
    """
    scopes: dict[int, dict[str, tuple[ast.AST, ...]]] = {}
    tables: dict[
        int, tuple[dict[str, tuple[ast.AST, ...]], dict[str, tuple[ast.AST, ...]]]
    ] = {}
    stack: list[tuple[ast.AST, dict[str, tuple[ast.AST, ...]]]] = [(tree, {})]
    while stack:
        node, overrides = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            declared = [
                name
                for name in (
                    getattr(p, "name", "") for p in getattr(node, "type_params", ())
                )
                if name
            ]
            if declared:
                params = _type_param_bindings(node)
                inside = (
                    {id(n) for n in ast.walk(node)}
                    if any(name in local for name in declared)
                    else set()
                )
                overrides = dict(overrides)
                for name in declared:
                    own = tuple(
                        value
                        for value in local.get(name, ())
                        if id(_origin(value)) in inside
                    )
                    overrides[name] = own + params.get(name, ())
        if overrides:
            # One merged table per override set, shared by all its nodes; the
            # set itself is kept alive next to it so its `id` is not reused.
            cached = tables.get(id(overrides))
            if cached is None:
                cached = tables[id(overrides)] = ({**local, **overrides}, overrides)
            scopes[id(node)] = cached[0]
        stack.extend((child, overrides) for child in ast.iter_child_nodes(node))
    return scopes


def _alias_values(
    name: str,
    aliases: dict[str, str],
    local: dict[str, tuple[ast.AST, ...]],
    seen: frozenset[str],
) -> tuple[ast.AST, ...]:
    """Every value `name` or its import alias binds locally, `()` inside a cycle.

    `seen` carries the keys of the alias table, so a cycle without a base case
    (`type C1 = C2`, `type C2 = C1`) ends that branch instead of the process. It
    is not reported: such a cycle names no class an instance can have, and a
    recursive alias *with* a base case (`type R = ClientError | list[R]`) keeps
    its other members and is reported through them.
    """
    if not name or name in seen:
        return ()
    values = tuple(local.get(name, ()))
    resolved = aliases.get(name, name)
    if resolved != name and resolved not in seen:
        values += tuple(local.get(resolved, ()))
    return values


def _is_foreign_exception_annotation(
    annotation: ast.AST | None,
    own: frozenset[str],
    aliases: dict[str, str] | None = None,
    imported: frozenset[str] = frozenset(),
    modules: dict[str, str] | None = None,
    *,
    local: dict[str, tuple[ast.AST, ...]],
) -> bool:
    """A parameter annotated with a broad or foreign exception type.

    `err: BaseException`, `err: ClientError`, `err: ssl.SSLError | None`,
    `err: Optional[OSError]`: the callee logs a producer's exception it did
    not catch itself, so the name is bound outside any `except` and the
    handler walk alone would not see it (found in
    `fcm_receiver_ha._classify_registration_exception`). An own type
    (`err: FatalRegistrationError`) is not foreign; `type[X]` and
    `Callable[..., X]` carry a class, not an instance.

    `local` has no default on purpose: it is the table of the module's own type
    aliases, and a forgotten hand-over would silently classify every alias as
    harmless. Keyword-only, because the parameters before it carry defaults and
    a positional one without a default cannot follow them; the effect is the
    one asked for, a missing hand-over is a `TypeError` and not a quiet `False`.
    """
    members = _annotation_members(annotation, aliases, local)
    for member in members:
        if member is _ALIAS_LIMIT_MEMBER:
            # The fail-closed member of a truncated expansion, matched by
            # identity and before anything else: a module that binds the name
            # `BaseException` itself (`class BaseException(Exception)`, `from .
            # import BaseException`) would otherwise make it *own* and swallow
            # the assurance the two limits give. Measured: without this line, a
            # 70-hop chain goes silent as soon as such a binding is present.
            return True
        if _is_qualified_exception(member, modules or {}):
            return True
        chain = _chain(member)
        # `_chain` lists the attribute first (`ssl.SSLError` ->
        # ["SSLError", "ssl"]), the same reading `_is_foreign_handler` uses.
        name = chain[0] if chain else ""
        if name in own:
            continue
        if name in imported:
            return True
        resolved = (aliases or {}).get(name, name)
        if (
            resolved in _BROAD_EXCEPTIONS
            or resolved in _BUILTIN_EXCEPTIONS
            or resolved.endswith(_EXCEPTION_SUFFIXES)
        ):
            return True
    return False


# Generic heads whose members are instances: a union, or a container whose
# `%s` renders every element with its text; a mapping renders keys and
# values (`{OSError('text'): 1}`), so both slots count, and so do the
# stdlib containers (`defaultdict[str, OSError]`, `deque[OSError]`,
# `Counter[OSError]`; Codex review on 714596f), a mapping view, a mapping
# proxy, an `asyncio` future, task or queue (`<Future finished
# result=OSError('text')>`, `<Queue maxsize=0 _queue=[OSError('text')]>`,
# `LifoQueue` and `PriorityQueue` inherit that rendering),
# `Container` and `Reversible` (a list satisfies both). `Annotated[X, ...]`
# carries `X` in its first slot. Not carriers, because their instances do
# not render their members: `Awaitable`, `Coroutine`, `Generator`,
# `AsyncIterator`, `weakref.ref`, `AbstractContextManager`. Any other head
# (`ExceptionGroup[OSError]`) is checked as a name itself; `type[X]` and
# `Callable[..., X]` fall through that check because neither head is an
# exception. `test_member_carriers_expose_their_members` pins every entry.
_MEMBER_CARRIERS = frozenset(
    {
        "Optional",
        "Union",
        "list",
        "tuple",
        "set",
        "frozenset",
        "List",
        "Tuple",
        "Set",
        "FrozenSet",
        "Sequence",
        "Iterable",
        "Collection",
        "Iterator",
        "dict",
        "Dict",
        "Mapping",
        "MutableMapping",
        "MutableSequence",
        "MutableSet",
        "AbstractSet",
        "KeysView",
        "ValuesView",
        "ItemsView",
        "defaultdict",
        "DefaultDict",
        "deque",
        "Deque",
        "OrderedDict",
        "Counter",
        "ChainMap",
        "MappingProxyType",
        "Future",
        "Task",
        "Queue",
        "LifoQueue",
        "PriorityQueue",
        "Container",
        "Reversible",
    }
)


def _subscript_slots(head_name: str, inner: list[ast.AST]) -> list[ast.AST] | None:
    """The slots a subscript head exposes, the one rule for both readings.

    `Annotated[X, ...]` and `Unpack[X]` (PEP 646) expose their first slot, a
    carrier all of them, any other head none: `type[X]` and `Callable[..., X]` carry a class, not an instance,
    and `ExceptionGroup[OSError]` is checked as a name itself. `None` means "no
    slot counts, the head is the member". The alias branch applies this same
    rule to the head the alias resolves to, so a later change here carries both
    readings at once.
    """
    if head_name in ("Annotated", "Unpack"):
        return inner[:1]
    if head_name in _MEMBER_CARRIERS:
        return inner
    return None


def _annotation_members(
    annotation: ast.AST | None,
    aliases: dict[str, str] | None = None,
    local: dict[str, tuple[ast.AST, ...]] | None = None,
    seen: frozenset[str] = frozenset(),
) -> list[ast.AST]:
    """Flatten unions, `Optional[A]`, `Union[A, B]`, containers and `"A | B"`.

    A carrier head is resolved through the import-alias table first
    (`from typing import Optional as Maybe`, `Maybe[ClientError]`), and a local
    type alias is expanded through `local` (`type A = ClientError`, `err: A`),
    the head included (`type Errs = list`, `err: Errs[ClientError]`).

    The expansion is **additive**: the annotation stays a member and the alias
    values are added to it. A replacing expansion would report less than today,
    which a guard must never do when it resolves more: `type HarmlessError =
    str` and `type TimeoutError = str` are reported by their name, and
    `Counter = MyCounter` with `err: Counter[ClientError]` is reported because
    `Counter` is a carrier; all three would go silent.

    A generic alias is applied to its arguments (`_apply_generic_alias`): in
    `type Maybe[T] = T | None` with `err: Maybe[ClientError]`, `T` stands for
    `ClientError` wherever the value places it, and nowhere else, so
    `Callable[[T], None]` stays as silent as when written out. A union is
    flattened without recursion, so its width costs no frames, and a starred
    or `Unpack`ed slot (`tuple[*tuple[ClientError]]`) counts like the slot.

    Two limits, both fail-closed with a synthetic `BaseException` member and
    both set rather than derived: `_ALIAS_MEMBER_LIMIT` on the width, because
    additive expansion of a doubling chain grows as `3*2^n - 1`, and
    `_ALIAS_DEPTH_LIMIT` on the alias hops, which tightens the cut of a linear
    chain from 512 hops to 64 and so keeps the nesting well inside the
    interpreter's recursion limit (measured: 521 of 1000 frames with the width
    limit alone, 73 with both). The fail-closed member is matched by identity, not by its
    name, so a module that binds `BaseException` itself cannot make it own. The
    budget is carried *into* the recursion and checked on entry; trimming the
    returned list afterwards would cap the result and not the work (4194302
    calls measured at n=20, 710 with the budget).
    """
    members: list[ast.AST] = []
    _collect_annotation_members(
        annotation, aliases or {}, local or {}, seen, members, _NO_ENV
    )
    return members


def _collect_annotation_members(
    annotation: ast.AST | None,
    aliases: dict[str, str],
    local: dict[str, tuple[ast.AST, ...]],
    seen: frozenset[str],
    members: list[ast.AST],
    env: Mapping[str, tuple[_Binding, ...]],
) -> None:
    """Append one annotation's members to `members`, honouring both limits.

    `env` binds the type parameters of the generic alias whose value is being
    walked; it is empty everywhere else. A plain alias value starts with an
    empty one and a generic alias's value with the bindings of its own
    parameters, never with the caller's: it sees its own parameters and
    nothing of the annotation that uses it.
    """
    if annotation is None:
        return
    if len(members) > _ALIAS_MEMBER_LIMIT or len(seen) > _ALIAS_DEPTH_LIMIT:
        # Fail-closed and once: every further call returns here, so the work
        # stops with the list instead of walking an exponential expansion.
        if not members or members[-1] is not _ALIAS_LIMIT_MEMBER:
            members.append(_ALIAS_LIMIT_MEMBER)
        return
    while isinstance(annotation, ast.Starred):
        annotation = annotation.value
    if isinstance(annotation, ast.Constant) and isinstance(annotation.value, str):
        parsed = _parse_type_string(annotation.value)
        if parsed is None:
            return
        annotation = parsed
    if isinstance(annotation, ast.BinOp) and isinstance(annotation.op, ast.BitOr):
        # Flattened on an explicit stack: a union of 1000 members nests 1000
        # deep and ended the scan with a `RecursionError` while this branch
        # recursed.
        stack: list[ast.AST] = [annotation]
        operands: list[ast.AST] = []
        while stack:
            node = stack.pop()
            if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
                stack += (node.right, node.left)
            else:
                operands.append(node)
        for operand in operands:
            _collect_annotation_members(operand, aliases, local, seen, members, env)
        return
    if isinstance(annotation, ast.Subscript):
        chain = _chain(annotation.value)
        head = chain[0] if chain else ""
        head_name = aliases.get(head, head)
        inner = (
            list(annotation.slice.elts)
            if isinstance(annotation.slice, ast.Tuple)
            else [annotation.slice]
        )
        # The reading without aliases, unchanged: it decides on the annotation
        # head (`Errs` in `err: Errs[ClientError]`).
        slots = _subscript_slots(head_name, inner)
        if slots is None:
            members.append(annotation.value)
        else:
            for item in slots:
                _collect_annotation_members(item, aliases, local, seen, members, env)
        # The alias branch on top, never instead: the value head (`list` in
        # `Errs = list`) decides which slots the alias exposes, and a generic
        # alias binds its parameters to them instead.
        values = _alias_values(head, aliases, local, seen)
        if values:
            if slots is not None:
                members.append(annotation.value)
            deeper = seen | {head, head_name}
            for value in values:
                if not isinstance(value, _GenericAlias):
                    _collect_annotation_members(
                        value, aliases, local, deeper, members, _NO_ENV
                    )
            head_slots, generics = _alias_head_slots(
                values, inner, aliases, local, deeper
            )
            # The slots belong to this annotation, not to the alias value, so
            # they keep the caller's `seen`: `M[str, M[str, ClientError]]`
            # repeats `M` without being a cycle.
            for item in head_slots:
                _collect_annotation_members(item, aliases, local, seen, members, env)
            for generic in generics:
                _apply_generic_alias(
                    generic, inner, env, seen, aliases, local, deeper, members
                )
        return
    if isinstance(annotation, ast.Name) and annotation.id in env:
        # A parameter of the alias being walked: it stands for its bindings,
        # each read where it was written, and shadows any alias of the same
        # name. Its own name is no member, `_PARAM_MEMBER` counts in its place.
        members.append(_PARAM_MEMBER)
        for binding in env[annotation.id]:
            _collect_annotation_members(
                binding.node, aliases, local, binding.seen, members, binding.env
            )
        return
    members.append(annotation)
    # The outermost name of an attribute chain, so `err: Holder.InClass` reaches
    # the table too; `_chain` unwraps a call to its head.
    chain = _chain(annotation)
    name = chain[0] if chain else ""
    values = _alias_values(name, aliases, local, seen)
    if values:
        deeper = seen | {name, aliases.get(name, name)}
        for value in values:
            if isinstance(value, _GenericAlias):
                _apply_generic_alias(
                    value, [], _NO_ENV, seen, aliases, local, deeper, members
                )
            else:
                _collect_annotation_members(
                    value, aliases, local, deeper, members, _NO_ENV
                )


def _alias_head_slots(
    values: tuple[ast.AST, ...],
    inner: list[ast.AST],
    aliases: dict[str, str],
    local: dict[str, tuple[ast.AST, ...]],
    seen: frozenset[str],
) -> tuple[list[ast.AST], list[_GenericAlias]]:
    """The slots of `inner` that an alias head exposes, and the generic aliases it reaches.

    `type Hd0 = list`, `type Hd1 = Hd0`, `err: Hd1[ClientError]`: the value
    head `Hd0` is itself an alias, and its value `list` decides. A generic
    alias on the way exposes no slot: `inner` binds its parameters, which the
    caller does with the returned aliases (`type Meta[T] = Annotated[int, T]`
    with `Meta[ClientError]` is silent, not the first slot of `Annotated`). The
    heads are walked with one visited set rather than a path, so every name is
    expanded once and the work is linear in the number of definitions, and
    without recursion, so a long chain costs no frames.
    """
    slots: list[ast.AST] = []
    generics: list[_GenericAlias] = []
    visited = set(seen)
    pending = list(values)
    while pending:
        value = pending.pop()
        if isinstance(value, _GenericAlias):
            generics.append(value)
            continue
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            parsed = _parse_type_string(value.value)
            if parsed is None:
                continue
            value = parsed
        value_chain = _chain(value)
        value_head = value_chain[0] if value_chain else ""
        slots.extend(_subscript_slots(aliases.get(value_head, value_head), inner) or [])
        if not isinstance(value, (ast.Name, ast.Attribute)):
            continue
        for name in (value_head, aliases.get(value_head, value_head)):
            if name and name not in visited:
                visited.add(name)
                pending.extend(local.get(name, ()))
    return slots, generics


def _target_names(target: ast.AST) -> tuple[str, ...]:
    """Every name a binding target writes: `a`, `self.last`, `(a, *b)`, `d["k"]` and `d`."""
    if isinstance(target, ast.Name):
        return (target.id,)
    if isinstance(target, ast.Attribute):
        return (ast.unparse(target),)
    if isinstance(target, ast.Subscript):
        # An element write binds the container too (`d["k"] = err` renders
        # `d`), whether the target stands alone, in a tuple or under `+=`.
        return (ast.unparse(target), *_container_names(target.value))
    if isinstance(target, ast.Starred):
        return _target_names(target.value)
    if isinstance(target, (ast.Tuple, ast.List)):
        return tuple(n for elt in target.elts for n in _target_names(elt))
    return ()


def _mentions_exception(expr: ast.AST, bound: frozenset[str]) -> bool:
    """A bound name occurs in the expression outside a safe wrapper or code attribute."""
    return any(_exception_leaves(expr, name) for name in bound)


def _carries_exception(value: ast.AST, bound: frozenset[str]) -> bool:
    """The value renders a bound exception or `<task>.exception()` when logged.

    Followed: the name itself or its attribute other than a code
    (`err.args`, `err.strerror`, `getattr(err, "msg")`), a text wrapper
    (`str(err)`, `f"{err}"`, `"x %s" % err`, `"x " + str(err)`,
    `", ".join(...)`), `sys.exc_info()`, the default slot of `d.get(k,
    err)`, a built-in that hands its arguments back
    (`list(errors)`, `map(str, errors)`, `next(iter(errors))`), the
    receiver of any method (`str(err).strip()`, `errors.pop()`), `or`/`and`
    (`str(err) or ""`), a conditional (`err if err else "none"`), a
    container display, a subscript (`errors[0]`), a comprehension whose
    element carries, a starred or awaited value. Not followed, on purpose:
    any other function that takes the name as an argument (`kind =
    _classify(entry_id, err)`, `len(errors)`: a function's result is not
    knowable from the name, and the result of a function this file does not
    define is a stated blind spot; a module-local helper whose `return`
    carries is followed through `returning`); binding such a target would
    report every logged result. Any
    call named `exception()` or `exc_info()` is read as the task method or
    `sys.exc_info()`, `_LOGGER.exception(...)` included: fail-closed, one
    review line. `operator.setitem(d, k, err)` and the like are functions,
    not methods, and fall under the sentence above.
    """
    parts: list[ast.AST] = []
    if isinstance(value, ast.Call):
        if getattr(value.func, "attr", "") == "exception":
            return True
        if getattr(value.func, "attr", "") == "exc_info":
            return True
        chain = _chain(value.func)
        if chain and chain[0] in _TEXT_WRAPPERS | _TRANSPARENT_CALLS:
            parts = [*value.args, *(kw.value for kw in value.keywords)]
        elif chain and chain[0] in _DEFAULT_METHODS:
            # `d.get(k, err)` and `m.get(k, default=err)` hand the default back;
            # every keyword counts (`Mapping.get` names it, a wrapper may not).
            parts = [*value.args[1:], *(kw.value for kw in value.keywords)]
        elif chain == ["getattr"] and len(value.args) >= 2:
            # `getattr(err, "msg")` reads what `err.msg` reads; only a code
            # attribute named literally is safe (`auth_flow.py:108`).
            attr = value.args[1]
            parts = list(value.args[2:])  # the default slot carries too
            if not (
                isinstance(attr, ast.Constant) and attr.value in _SAFE_EXCEPTION_ATTRS
            ):
                parts.append(value.args[0])
        if isinstance(value.func, ast.Attribute):
            # The receiver of any method carries into the result
            # (`str(err).strip()`, `errors.pop()`, `err.args[0].lower()`);
            # `describe_exception(err).split()` does not, its receiver is a
            # safe wrapper, and `err.errno.bit_length()` does not either.
            parts.append(value.func.value)
    elif isinstance(value, ast.BoolOp):
        parts = list(value.values)
    elif isinstance(value, (ast.Name, ast.Attribute)):
        if ast.unparse(value) in bound:
            return True
        if isinstance(value, ast.Attribute) and value.attr not in _SAFE_EXCEPTION_ATTRS:
            parts = [value.value]
    elif isinstance(value, (ast.Subscript, ast.Starred, ast.Await, ast.NamedExpr)):
        parts = [value.value]
    elif isinstance(value, ast.BinOp):
        parts = [value.left, value.right]
    elif isinstance(value, ast.IfExp):
        parts = [value.body, value.orelse]
    elif isinstance(value, ast.JoinedStr):
        return _mentions_exception(value, bound)
    elif isinstance(value, (ast.Tuple, ast.List, ast.Set)):
        parts = list(value.elts)
    elif isinstance(value, ast.Dict):
        parts = [part for part in [*value.keys, *value.values] if part is not None]
    elif isinstance(value, (ast.ListComp, ast.SetComp, ast.GeneratorExp, ast.DictComp)):
        # The comprehension target is bound by `_flow_bound_names` (it is an
        # `ast.comprehension` node), so the element decides at the fixpoint.
        parts = (
            [value.key, value.value] if isinstance(value, ast.DictComp) else [value.elt]
        )
    return any(_carries_exception(part, bound) for part in parts)


def _unpacked_names(
    target: ast.AST, value: ast.AST, bound: frozenset[str]
) -> tuple[str, ...]:
    """Names a binding writes from a carrier, matched by position where it can be.

    `err, count = task.exception(), 0` binds `err` and not `count`;
    `[first, *rest] = [task.exception(), 0]` binds `first` (the starred
    target collects the remainder, which carries when any of its elements
    does). A tuple target fed by anything but a tuple or list display
    binds every name when the value carries (`code, text = err.args`,
    `first, *rest = errors`: which slot holds the text is not knowable,
    so all of them do) and none when it does not (`a, b = helper(task)`).
    """
    if not isinstance(target, (ast.Tuple, ast.List)):
        return _target_names(target) if _carries_exception(value, bound) else ()
    if not isinstance(value, (ast.Tuple, ast.List)):
        return _target_names(target) if _carries_exception(value, bound) else ()
    targets, values = list(target.elts), list(value.elts)
    star = next((i for i, t in enumerate(targets) if isinstance(t, ast.Starred)), None)
    if star is None:
        if len(targets) != len(values):
            return ()
        pairs = list(zip(targets, values, strict=True))
        return tuple(n for t, v in pairs for n in _unpacked_names(t, v, bound))
    tail = len(targets) - star - 1
    if len(values) < len(targets) - 1:
        return ()
    head = list(zip(targets[:star], values[:star], strict=True))
    rear = list(zip(targets[star + 1 :], values[len(values) - tail :], strict=True))
    names = [n for t, v in head + rear for n in _unpacked_names(t, v, bound)]
    rest = values[star : len(values) - tail]
    if any(_carries_exception(v, bound) for v in rest):
        names.extend(_target_names(targets[star].value))
    return tuple(names)


def _container_names(expr: ast.AST) -> tuple[str, ...]:
    """The names an element write reaches: `d[k]` and `d`, `d.setdefault(k, []).append`'s `d`.

    A container whose element carries renders it whole, so the write to
    `d[k]`, to `self.errs["k"]` or through a method chain on `d` binds the
    container text as well as the element text. A call at the base
    (`get_list().append(err)`) names nothing.
    """
    if isinstance(expr, ast.Name):
        return (expr.id,)
    if isinstance(expr, ast.Attribute):
        return (ast.unparse(expr),)
    if isinstance(expr, ast.Subscript):
        return (ast.unparse(expr), *_container_names(expr.value))
    if isinstance(expr, ast.Call) and isinstance(expr.func, ast.Attribute):
        return _container_names(expr.func.value)
    return ()


def _capture_names(pattern: ast.AST) -> tuple[str, ...]:
    """Every name a `match` pattern captures (`as x`, `*rest`, `**rest`)."""
    return tuple(
        name
        for node in ast.walk(pattern)
        for name in (getattr(node, "name", None), getattr(node, "rest", None))
        if name
    )


def _flow_bound_names(
    function: ast.AST,
    seed: tuple[str, ...],
    own: frozenset[str],
    aliases: dict[str, str],
    imported: frozenset[str],
    modules: dict[str, str],
    local: dict[str, tuple[ast.AST, ...]],
    returning: frozenset[int] = frozenset(),
    yielding: frozenset[int] = frozenset(),
) -> tuple[str, ...]:
    """Fourth binding: names that receive a bound exception inside one function.

    Followed to a fixpoint, whole function body, nested scopes included:
    an assignment, annotated assignment, augmented assignment or walrus
    whose value carries a bound name or `<task>.exception()`
    (`_carries_exception`: `detail = exc`, `self.last = err`, `shown =
    str(err)`, `msg += str(err)`, `first = errors[0]`; the attribute or
    subscript target is read back by its dotted text), a mutation of a
    container by a method that stores its argument (`parts.append(str(err))`
    binds `parts`), tuple and list unpacking matched by position (starred
    included) or, from a carrying value that is no display, as a whole
    (`code, text = err.args`), a `for`, `async for` or comprehension
    target whose iterable mentions a bound name (`for err in errors`,
    `for i, err in enumerate(errors)`, `for e in self.errors`; the index
    of `enumerate` and of `range(len(errors))` is bound too, fail-closed),
    and a `match` capture inside a foreign class pattern (`case
    ClientError() as err`, `case ClientError(args=[first])`) or any capture
    when the subject mentions a bound name. Fail-closed on purpose: a
    rebinding to a harmless value (`err = "safe"`) and a nested function
    with its own parameter of the same name keep the name bound; neither
    has an instance today, and a false report there costs one review
    line, a missed one a producer's text in the log. A handler name seeds
    this flow only inside its handler (`_exception_names_by_call` passes
    the handler as the function); an alias that outlives the handler
    (`except Exception as exc: last_error = exc`) and a local annotation
    with a union (`last_error: str | Exception | None`) are not followed
    after it: the package narrows such a name before each of its three
    log calls (`fcmregister.py`: one by `isinstance`, two by a `str`
    assignment in the same branch), and a flow-insensitive binding would
    report all three. An assignment, annotated assignment or walrus whose
    value (behind `await`) is a call in `returning` binds its target as a
    whole: the call reaches a module-local helper whose `return` carries a
    bound exception (`_returning_calls`); a value that only contains such a
    call (`pick(task) or 'none'`) binds nothing. A `with` or `async with` item
    whose manager is a call in `yielding` binds its `as`
    target the same way: the manager is a module-local generator whose
    `yield` carries a bound exception (`_yielding_calls`).
    """
    bound = set(seed)
    while True:
        frozen = frozenset(bound)
        for node in ast.walk(function):
            if isinstance(node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
                if node.value is None:
                    continue
                targets = (
                    node.targets if isinstance(node, ast.Assign) else [node.target]
                )
                value = node.value
                if isinstance(value, ast.Await):
                    value = value.value
                for target in targets:
                    bound.update(_unpacked_names(target, node.value, frozen))
                    if id(value) in returning:
                        bound.update(_target_names(target))
            elif isinstance(node, (ast.With, ast.AsyncWith)):
                for item in node.items:
                    manager = item.context_expr
                    if item.optional_vars is not None and id(manager) in yielding:
                        bound.update(_target_names(item.optional_vars))
            elif isinstance(node, ast.AugAssign):
                if _carries_exception(node.value, frozen):
                    bound.update(_target_names(node.target))
            elif (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in _MUTATORS
            ):
                # A mutation is a binding of the receiver, wherever the call
                # sits (a statement, a comprehension): `parts.append(str(err))`
                # and `d.setdefault(k, []).append(err)` make `parts` and `d`
                # render the text when logged.
                if any(
                    _carries_exception(part, frozen)
                    for part in [*node.args, *(kw.value for kw in node.keywords)]
                ):
                    bound.update(_container_names(node.func.value))
            elif isinstance(node, (ast.For, ast.AsyncFor, ast.comprehension)):
                if _mentions_exception(node.iter, frozen):
                    bound.update(_target_names(node.target))
            elif isinstance(node, ast.Match):
                subject_bound = _mentions_exception(node.subject, frozen)
                for case in node.cases:
                    if subject_bound:
                        bound.update(_capture_names(case.pattern))
                        continue
                    for pattern in ast.walk(case.pattern):
                        if isinstance(pattern, ast.MatchClass) and (
                            _is_foreign_exception_annotation(
                                pattern.cls,
                                own,
                                aliases,
                                imported,
                                modules,
                                local=local,
                            )
                        ):
                            bound.update(_capture_names(pattern))
                        elif isinstance(pattern, ast.MatchAs) and pattern.name:
                            if any(
                                isinstance(cls, ast.MatchClass)
                                and _is_foreign_exception_annotation(
                                    cls.cls,
                                    own,
                                    aliases,
                                    imported,
                                    modules,
                                    local=local,
                                )
                                for cls in ast.walk(pattern)
                            ):
                                bound.add(pattern.name)
        if frozenset(bound) == frozen:
            return tuple(sorted(bound - set(seed)))


# Rounds of the module-wide fixpoint over helper calls; past it every parameter
# of every module-local callee is bound (fail-closed). A chain needs one round
# per hop plus one to confirm; measured on 2026-10-24, with helper results
# and manager yields in the fixpoint, every `Auth/` module settles within
# three rounds (two of the nineteen scanned modules need the third).
_FORWARD_ROUND_LIMIT = 64
_RECEIVERS = frozenset({"self", "cls"})


def _module_local_callees(
    tree: ast.AST,
) -> dict[str, list[tuple[ast.AST, int]]]:
    """Every `def` of the module by name, with the receiver slots it skips.

    A method (a `def` directly in a class body) skips its first parameter when
    called through `self.` or `cls.`, a `@staticmethod` skips none; a function
    defined anywhere else, nested ones included, is a callee by its bare name.
    The skip is `-1` for a function: a bare name never reaches a method.
    """
    methods: dict[int, int] = {}
    for klass in ast.walk(tree):
        if isinstance(klass, ast.ClassDef):
            for item in klass.body:
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    static = any(
                        _chain(d) == ["staticmethod"] for d in item.decorator_list
                    )
                    methods[id(item)] = 0 if static else 1
    callees: dict[str, list[tuple[ast.AST, int]]] = {}
    for function in ast.walk(tree):
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            callees.setdefault(function.name, []).append(
                (function, methods.get(id(function), -1))
            )
    return callees


def _resolved_callees(
    call: ast.Call, callees: dict[str, list[tuple[ast.AST, int]]]
) -> list[tuple[ast.AST, int]]:
    """The module-local `def`s a call may reach, with the receiver slots to skip.

    `h(err)` reaches every function named `h` that is not a method;
    `self.h(err)` and `cls.h(err)` reach every method named `h` in any class of
    the module (two classes with the same method name: both, fail-closed).
    Not followed: another receiver (`self._client.h(err)`), an imported
    helper, `functools.partial`, an alias of a function (`g = h; g(err)`), a
    lambda and a dispatch through `getattr`.
    """
    func = call.func
    if isinstance(func, ast.Name):
        return [(f, 0) for f, skip in callees.get(func.id, []) if skip < 0]
    if (
        isinstance(func, ast.Attribute)
        and isinstance(func.value, ast.Name)
        and func.value.id in _RECEIVERS
    ):
        return [(f, skip) for f, skip in callees.get(func.attr, []) if skip >= 0]
    return []


def _forwarded_parameter_seeds(
    tree: ast.AST,
    names: dict[int, tuple[str, ...]],
    callees: dict[str, list[tuple[ast.AST, int]]],
) -> dict[int, tuple[str, ...]]:
    """Parameters of module-local helpers that receive a bound exception.

    Every call whose argument carries a name bound at the call
    (`_carries_exception`: `describe(err)`, `describe(cause=err)`,
    `h(str(err))`, `h(*errors)`, `h(**{"cause": err})`; not
    `h(describe_exception(err))`) binds the parameter the argument lands in:
    by position (positional-only and normal parameters, the rest in
    `*args`), by keyword (normal and keyword-only parameters, the rest in
    `**kwargs`). A starred argument binds its slot and every positional slot
    after it, `**mapping` every parameter that takes a keyword; which slot
    the value reaches is not knowable from the call. Every parameter counts,
    whatever its annotation (`v: object`, `v: str`): the caller decides what
    it holds (decision V-4, 2026-10-22).
    """
    seeds: dict[int, set[str]] = {}
    for call in ast.walk(tree):
        if not isinstance(call, ast.Call):
            continue
        targets = _resolved_callees(call, callees)
        if not targets:
            continue
        bound = frozenset(names.get(id(call), ()))
        for callee, skip in targets:
            args = callee.args
            positional = [*args.posonlyargs, *args.args][skip:]
            variadic = [args.vararg.arg] if args.vararg else []
            # The receiver may be positional-only (`def h(self, /, v)`):
            # skip it in the joint list, not in `args.args`.
            keyword = {p.arg for p in [*positional, *args.kwonlyargs]}
            keyword -= {p.arg for p in args.posonlyargs}
            # A starred argument may unpack to nothing, so every argument
            # from the first one on may land in its slot (`h(*(), err)`).
            first_star: int | None = None
            hit: set[str] = set()
            for index, argument in enumerate(call.args):
                if first_star is None and isinstance(argument, ast.Starred):
                    first_star = index
                if not _carries_exception(argument, bound):
                    continue
                if first_star is not None:
                    hit.update(p.arg for p in positional[first_star:])
                    hit.update(variadic)
                elif index < len(positional):
                    hit.add(positional[index].arg)
                else:
                    hit.update(variadic)
            for kw in call.keywords:
                if not _carries_exception(kw.value, bound):
                    continue
                if kw.arg is None:
                    hit.update(keyword)
                    hit.update([args.kwarg.arg] if args.kwarg else [])
                elif kw.arg in keyword:
                    hit.add(kw.arg)
                elif args.kwarg:
                    hit.add(args.kwarg.arg)
            if hit:
                seeds.setdefault(id(callee), set()).update(hit)
    return {key: tuple(sorted(value)) for key, value in seeds.items()}


def _all_callee_parameters(
    tree: ast.AST, callees: dict[str, list[tuple[ast.AST, int]]]
) -> dict[int, tuple[str, ...]]:
    """Every parameter of every module-local `def` some call of the module reaches."""
    seeds: dict[int, tuple[str, ...]] = {}
    for call in ast.walk(tree):
        if isinstance(call, ast.Call):
            for callee, _skip in _resolved_callees(call, callees):
                args = callee.args
                seeds[id(callee)] = tuple(
                    p.arg
                    for p in [
                        *args.posonlyargs,
                        *args.args,
                        *args.kwonlyargs,
                        *([args.vararg] if args.vararg else []),
                        *([args.kwarg] if args.kwarg else []),
                    ]
                )
    return seeds


def _exception_names_by_call(
    tree: ast.AST, carrying: set[int]
) -> dict[int, tuple[str, ...]]:
    """Map each call to every foreign exception name in scope, `()` if none.

    Five bindings, accumulated rather than replaced: every parameter annotated
    with a foreign exception type (whole function body, all of them, not the
    first: `def f(first: BaseException, second: OSError)` logs `second`), a
    parameter of a module-local helper that a call passes a bound name to
    (`_forwarded_parameter_seeds`, followed to a module-wide fixpoint over
    any number of hops, recursion and cycles included), a name that receives
    such a parameter or `<task>.exception()` through data flow
    (`_flow_bound_names`: assignment, unpacking, `for`, `match`, whole
    function body), and a foreign `except … as NAME` handler (handler body,
    with the same data flow bounded to that body).
    A call inside a foreign handler without `as` keeps the names the
    enclosing function bound (`def f(err: BaseException): … except
    Exception: log(err)` keeps `err`); the implicit traceback of such a
    handler (`logger.exception`, `exc_info=True`) is reported positionally,
    not through this map. Past `_FORWARD_ROUND_LIMIT` rounds every parameter
    of every module-local callee is bound (fail-closed).
    `carrying` receives the calls of module-local helpers whose
    `return` carries at the fixpoint (fail-closed: every such call that returns
    a value); `_returned_exception_leaves` reads them where no name binds them.
    """
    own = _own_type_names(tree)
    aliases = _import_aliases(tree)
    # One pass per file, before any annotation is classified: an alias may be
    # defined after its use, or inside a function or a class body.
    local = _local_type_aliases(tree, aliases)
    scopes = _type_param_scopes(tree, local)
    imported = _imported_exception_types(tree)
    modules = _module_bindings(tree)
    callees = _module_local_callees(tree)
    context = (own, aliases, local, scopes, imported, modules)
    forwarded: dict[int, tuple[str, ...]] = {}
    returning: frozenset[int] = frozenset()
    yielding: frozenset[int] = frozenset()
    for _round in range(_FORWARD_ROUND_LIMIT):
        by_scope: dict[int, tuple[str, ...]] = {}
        names = _names_for_seeds(
            tree, forwarded, context, returning, by_scope, yielding
        )
        seeds = _forwarded_parameter_seeds(tree, names, callees)
        calls = _returning_calls(tree, by_scope, callees, returning)
        managers = _yielding_calls(tree, by_scope, callees, returning)
        if managers != yielding:
            yielding = managers
            forwarded, returning = seeds, calls
            continue
        if seeds == forwarded and calls == returning:
            carrying.update(returning)
            return names
        forwarded, returning = seeds, calls
    every_returning = _all_returning_calls(tree, callees)
    carrying.update(every_returning)
    # The fallback round is the last one: nothing reads its scope names.
    return _names_for_seeds(
        tree,
        _all_callee_parameters(tree, callees),
        context,
        every_returning,
        {},
        _all_yielding_calls(tree, callees),
    )


def _scope_returns(
    node: ast.AST, handlers: tuple[int, ...] = ()
) -> Iterator[tuple[ast.Return, tuple[int, ...]]]:
    """The `return` statements of one function, with the handlers around each.

    A nested `def` or class is its own scope and not entered: its `return`
    does not make the enclosing function return anything. Only statements,
    handlers and `case` blocks are entered, iteratively: a `return` stands
    nowhere else, and an expression may nest deeper than the recursion limit
    (a long `|` chain in a type alias).
    """
    stack = [(child, handlers) for child in ast.iter_child_nodes(node)]
    while stack:
        child, around = stack.pop()
        if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        if not isinstance(child, (ast.stmt, ast.excepthandler, ast.match_case)):
            continue
        if isinstance(child, ast.Return):
            yield child, around
            continue
        inner = (*around, id(child)) if isinstance(child, ast.ExceptHandler) else around
        stack.extend((grand, inner) for grand in ast.iter_child_nodes(child))


def _returning_calls(
    tree: ast.AST,
    by_scope: dict[int, tuple[str, ...]],
    callees: dict[str, list[tuple[ast.AST, int]]],
    returning: frozenset[int],
) -> frozenset[int]:
    """Calls that reach a module-local helper whose `return` carries an exception.

    A helper returns an exception when one of its own `return` statements
    carries a name bound in the helper (`_carries_exception` over its
    parameters, forwarded parameters and data flow, plus the name of a
    foreign handler around the `return`: `except ClientError as caught:
    return caught`) or returns, behind `await`, a call that already is such
    a call (`return pick(task)`, one more hop per round). A helper named
    like a safe wrapper (`describe_exception`) is not one; neither is a
    function the module does not define (`_classify(err)`, an imported
    helper): one file does not show what it returns.
    """
    returners: set[int] = set()
    for function in ast.walk(tree):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if function.name in _SAFE_EXCEPTION_WRAPPERS:
            continue
        own = by_scope.get(id(function), ())
        for node, handlers in _scope_returns(function):
            if node.value is None:
                continue
            value = (
                node.value.value if isinstance(node.value, ast.Await) else node.value
            )
            bound = frozenset(
                [*own, *(n for h in handlers for n in by_scope.get(h, ()))]
            )
            if id(value) in returning or _carries_exception(value, bound):
                returners.add(id(function))
                break
    return frozenset(
        id(call)
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and any(id(f) in returners for f, _ in _resolved_callees(call, callees))
    )


def _scope_yields(
    node: ast.AST,
) -> Iterator[tuple[ast.Yield, tuple[int, ...]]]:
    """The `yield` expressions of one function, with the handlers around each.

    Like `_scope_returns`, but a `yield` is an expression and may stand
    inside one (`sent = yield err`), so expressions are entered too, still
    iteratively; a nested `def` or class is its own scope.
    """
    stack = [(child, ()) for child in ast.iter_child_nodes(node)]
    while stack:
        child, around = stack.pop()
        if isinstance(child, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if isinstance(child, ast.Yield):
            yield child, around
        inner = (*around, id(child)) if isinstance(child, ast.ExceptHandler) else around
        stack.extend((grand, inner) for grand in ast.iter_child_nodes(child))


def _yielding_calls(
    tree: ast.AST,
    by_scope: dict[int, tuple[str, ...]],
    callees: dict[str, list[tuple[ast.AST, int]]],
    returning: frozenset[int],
) -> frozenset[int]:
    """Calls that reach a module-local generator whose `yield` carries an exception.

    Such a generator, wrapped by `contextlib.contextmanager` or
    `asynccontextmanager`, hands what it yields to `with … as NAME`. Read
    like `_returning_calls`: a `yield` carries a name bound in the generator,
    the name of a foreign handler around it, or a call that returns an
    exception (`yield pick(task)`, behind `await`). The decorator is not checked
    (fail-closed: an undecorated generator in a `with` fails at runtime).
    A manager the module does not define (a parameter `cm`, `async with
    session.post(...) as resp`) is not one: one file does not show what its
    `__enter__` returns.
    """
    yielders: set[int] = set()
    for generator in ast.walk(tree):
        if not isinstance(generator, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        names = by_scope.get(id(generator), ())
        for node, handlers in _scope_yields(generator):
            if node.value is None:
                continue
            yielded = node.value
            if isinstance(yielded, ast.Await):
                yielded = yielded.value
            carried = frozenset(
                [*names, *(n for h in handlers for n in by_scope.get(h, ()))]
            )
            if id(yielded) in returning or _carries_exception(yielded, carried):
                yielders.add(id(generator))
                break
    return frozenset(
        id(call)
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and any(id(f) in yielders for f, _ in _resolved_callees(call, callees))
    )


def _all_yielding_calls(
    tree: ast.AST, callees: dict[str, list[tuple[ast.AST, int]]]
) -> frozenset[int]:
    """Every call of a module-local generator that yields a value (fail-closed)."""
    yielders = {
        id(function)
        for function in ast.walk(tree)
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef))
        and any(node.value is not None for node, _ in _scope_yields(function))
    }
    return frozenset(
        id(call)
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and any(id(f) in yielders for f, _ in _resolved_callees(call, callees))
    )


def _all_returning_calls(
    tree: ast.AST, callees: dict[str, list[tuple[ast.AST, int]]]
) -> frozenset[int]:
    """Every call of a module-local helper that returns a value (fail-closed)."""
    returners = {
        id(function)
        for function in ast.walk(tree)
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef))
        and function.name not in _SAFE_EXCEPTION_WRAPPERS
        and any(node.value is not None for node, _ in _scope_returns(function))
    }
    return frozenset(
        id(call)
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and any(id(f) in returners for f, _ in _resolved_callees(call, callees))
    )


def _names_for_seeds(
    tree: ast.AST,
    forwarded: dict[int, tuple[str, ...]],
    context: tuple,
    returning: frozenset[int],
    by_scope: dict[int, tuple[str, ...]],
    yielding: frozenset[int] = frozenset(),
) -> dict[int, tuple[str, ...]]:
    """One round of `_exception_names_by_call` for the given forwarded parameters.

    `returning` are the calls whose result binds (`_returning_calls`),
    `yielding` the managers whose `with … as` target binds (`_yielding_calls`);
    `by_scope` receives the names bound in each function and
    each foreign handler, keyed by the node's `id`.
    """
    own, aliases, local, scopes, imported, modules = context
    names: dict[int, tuple[str, ...]] = {}
    for function in ast.walk(tree):
        if not isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        params = [
            *function.args.posonlyargs,
            *function.args.args,
            *function.args.kwonlyargs,
            *([function.args.vararg] if function.args.vararg else []),
            *([function.args.kwarg] if function.args.kwarg else []),
        ]
        scoped = scopes.get(id(function), local)
        bound = tuple(
            p.arg
            for p in params
            if _is_foreign_exception_annotation(
                p.annotation, own, aliases, imported, modules, local=scoped
            )
        )
        bound += tuple(
            name for name in forwarded.get(id(function), ()) if name not in bound
        )
        # The data flow reads the table for `match` class patterns only, where
        # a type parameter cannot stand (`case T():` raises at runtime).
        bound += _flow_bound_names(
            function, bound, own, aliases, imported, modules, local, returning, yielding
        )
        by_scope[id(function)] = bound
        if not bound:
            continue
        for inner in ast.walk(function):
            if isinstance(inner, ast.Call):
                names[id(inner)] = names.get(id(inner), ()) + bound
    for handler in ast.walk(tree):
        if not isinstance(handler, ast.ExceptHandler):
            continue
        if not _is_foreign_handler(handler, own):
            continue
        # The handler name seeds the same data flow, bounded to the handler
        # body: `except OSError as exc: text = str(exc); log(text)` binds
        # `text` there and not after the handler (see the module docstring
        # on `fcmregister.py`, where the alias outlives the handler and is
        # narrowed before it is logged).
        bound = (handler.name,) if handler.name else ()
        if handler.name:
            bound += _flow_bound_names(
                handler,
                bound,
                own,
                aliases,
                imported,
                modules,
                local,
                returning,
                yielding,
            )
        by_scope[id(handler)] = bound
        for inner in ast.walk(handler):
            if isinstance(inner, ast.Call):
                names[id(inner)] = names.get(id(inner), ()) + bound
    return names


def _exception_leaves(argument: ast.AST, name: str) -> list[tuple[str, str]]:
    """Shape (I): NAME, `str(NAME)`, `_clip(NAME)`, `f"{NAME}"`, `NAME.args`.

    NAME may be dotted or subscripted text (`self.last`, `self.errs["k"]`)
    when the binding wrote such a target; it is matched by its unparsed text.

    Not reported: NAME inside a safe wrapper (`describe_exception(NAME)`,
    `type(NAME).__name__`, `isinstance(NAME, X)`), a code attribute
    (`NAME.errno`), and the keyword `exc_info` (the contract allows a
    traceback; that it carries the same text is a contract finding, not a
    guard one). The wrapper clears only its own arguments: in
    `f"{describe_exception(exc)} raw={exc}"` the second `exc` is reported.
    """
    cleared: set[int] = set()
    for node in ast.walk(argument):
        if isinstance(node, ast.Call):
            chain = _chain(node.func)
            if chain and chain[0] in _SAFE_EXCEPTION_WRAPPERS:
                for inner in node.args:
                    cleared.update(id(n) for n in ast.walk(inner))
        elif (
            isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == name
            and node.attr in _SAFE_EXCEPTION_ATTRS
        ):
            cleared.add(id(node.value))
    for node in ast.walk(argument):
        if isinstance(node, ast.Name) and node.id == name and id(node) not in cleared:
            return [("I", name)]
        if (
            isinstance(node, (ast.Attribute, ast.Subscript))
            and not name.isidentifier()
            and ast.unparse(node) == name
            and id(node) not in cleared
        ):
            return [("I", name)]
    return []


def _retrieved_exception_leaves(argument: ast.AST) -> list[tuple[str, str]]:
    """Shape (I): `task.exception()` or `sys.exc_info()` passed to the log call directly.

    The done callback has no name to bind when it writes
    `_LOGGER.error("%s", t.exception())`; the call itself is the leaf, and so
    is `sys.exc_info()` (its second slot is the exception).
    """
    cleared: set[int] = set()
    for node in ast.walk(argument):
        if isinstance(node, ast.Call):
            chain = _chain(node.func)
            if chain and chain[0] in _SAFE_EXCEPTION_WRAPPERS:
                for inner in node.args:
                    cleared.update(id(n) for n in ast.walk(inner))
    for node in ast.walk(argument):
        if (
            isinstance(node, ast.Call)
            and getattr(node.func, "attr", "") in {"exception", "exc_info"}
            and id(node) not in cleared
        ):
            return [("I", f"{node.func.attr}()")]
    return []


def _returned_exception_leaves(
    argument: ast.AST, carrying: set[int], bound: tuple[str, ...]
) -> list[tuple[str, str]]:
    """Shape (I): the result of a module-local helper whose `return` carries, used in place.

    `_LOGGER.error("%s", pick(task))` has no name to bind; like
    `task.exception()` passed directly, the call is the leaf, wherever it
    sits in the argument (`str(pick(task))`, `f"{pick(task)}"`,
    `pick(task).args`, `pick(task) or None`). Not reported: the call inside a
    safe wrapper, under a code attribute (`pick(task).errno`), and a call
    whose own arguments carry a bound name (the name is the leaf already).
    """
    cleared: set[int] = set()
    for node in ast.walk(argument):
        if isinstance(node, ast.Call):
            chain = _chain(node.func)
            if chain and chain[0] in _SAFE_EXCEPTION_WRAPPERS:
                for inner in node.args:
                    cleared.update(id(n) for n in ast.walk(inner))
        elif isinstance(node, ast.Attribute) and node.attr in _SAFE_EXCEPTION_ATTRS:
            cleared.add(id(node.value))
    names = frozenset(bound)
    for node in ast.walk(argument):
        if (
            isinstance(node, ast.Call)
            and id(node) in carrying
            and id(node) not in cleared
            and not _mentions_exception(node, names)
        ):
            return [("I", f"{ast.unparse(node.func)}()")]
    return []


def _is_exception_constructor(
    value: ast.AST, targets: list[ast.expr], raised_names: set[str]
) -> bool:
    """`X(...)` bound to a name that is raised later or is named like one."""
    if not isinstance(value, ast.Call):
        return False
    chain = _chain(value.func)
    if not chain:
        return False
    if chain[0].endswith(_EXCEPTION_SUFFIXES):
        return True
    return any(
        isinstance(target, ast.Name) and target.id in raised_names for target in targets
    )


def _scan_tree(
    tree: ast.AST,
    relative: str,
    reviewed: frozenset[tuple[str, str, str]] = frozenset(),
    rotating: frozenset[str] = frozenset(),
) -> tuple[list[tuple[str, int, str, str]], int]:
    """Offenders and scanned log calls of one parsed module."""
    offenders: list[tuple[str, int, str, str]] = []
    scanned = 0
    # Body names (shape (D)) and message parameters (shape (G)) are
    # scoped to their function.
    bodies_by_call: dict[int, frozenset[str]] = {}
    messages_by_call: dict[int, frozenset[str]] = {}
    aliases_by_call: dict[int, frozenset[str]] = {}
    message_types = _message_type_names(tree)
    for function in ast.walk(tree):
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            messages = _message_names(function, message_types)
            bodies = _body_names(function, messages)
            aliases = _log_aliases(function)
            if bodies or messages or aliases:
                # Union, not overwrite: a nested function keeps the bodies
                # and message parameters of the function that encloses it.
                for inner in ast.walk(function):
                    if isinstance(inner, ast.Call):
                        bodies_by_call[id(inner)] = (
                            bodies_by_call.get(id(inner), frozenset()) | bodies
                        )
                        messages_by_call[id(inner)] = (
                            messages_by_call.get(id(inner), frozenset()) | messages
                        )
                        aliases_by_call[id(inner)] = (
                            aliases_by_call.get(id(inner), frozenset()) | aliases
                        )
    in_scope_for_i = relative.startswith(_SHAPE_I_ROOT)
    carrying: set[int] = set()
    exception_names = _exception_names_by_call(tree, carrying) if in_scope_for_i else {}
    raised_names = {
        node.exc.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Name)
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Raise) and isinstance(node.exc, ast.Call):
            # Shape (H): an exception message is a log sink one hop later
            # (`_LOGGER.error("...: %s", err)` is the usual consumer), so the
            # constructor arguments are scanned like log arguments. The
            # format string is the marker `raise` so a reviewed pin can
            # single the site out.
            call = node.exc
            fmt = "raise"
        elif isinstance(node, ast.Assign) and _is_exception_constructor(
            node.value, node.targets, raised_names
        ):
            # Shape (H), two-step form: `err = X(...)` followed by
            # `raise err` (or a name that says it is an exception).
            call = node.value
            fmt = "raise"
        elif isinstance(node, ast.Call) and _is_log_call(
            node, aliases_by_call.get(id(node), frozenset())
        ):
            call = node
            # `.log(level, msg, ...)` carries the format string at index 1.
            msg_index = 1 if getattr(node.func, "attr", "") == "log" else 0
            first = node.args[msg_index] if len(node.args) > msg_index else None
            fmt = (
                first.value
                if isinstance(first, ast.Constant) and isinstance(first.value, str)
                else ""
            )
        else:
            continue
        scanned += 1
        bodies = bodies_by_call.get(id(call), frozenset())
        messages = messages_by_call.get(id(call), frozenset())
        # Shape (I) reads an exception constructor like a log call: an own
        # exception built from a foreign one (`raise Own(str(err)) from err`,
        # `wrapped = Own(err)`) carries the producer's text one hop later.
        bound_names = exception_names.get(id(call), ())
        arguments: list[ast.AST] = [*call.args, *(kw.value for kw in call.keywords)]
        # Shape (I) treats a traceback as a sink of its own: the last line of
        # a rendered traceback is `str(exc)`. `exc_info=<name>`, `exc_info=True`
        # and the implicit form `logger.exception(...)` inside a foreign
        # handler are reported under the leaf `exc_info`; `exc_info=False`
        # and `exc_info=None` are not. Shapes (A) to (H) scan the keyword's
        # value like any other argument.
        traceback_args = {id(kw.value) for kw in call.keywords if kw.arg == "exc_info"}
        if (
            in_scope_for_i
            and fmt != "raise"
            and not _is_reviewed(reviewed, relative, "exc_info", fmt)
        ):
            # Any handler, any value: an alias bound outside the handler
            # (`last_error = exc`), a tuple, `sys.exc_info()`, `1`, or an own
            # exception raised `from` a foreign one (the chained cause is
            # rendered too) all carry the producer's text; only `False` and
            # `None` do not. The rule is therefore positional, not semantic.
            implicit = getattr(call.func, "attr", "") == "exception"
            explicit = any(
                kw.arg == "exc_info"
                and not (
                    isinstance(kw.value, ast.Constant)
                    and kw.value.value in (False, None)
                )
                for kw in call.keywords
            )
            if implicit or explicit:
                offenders.append((relative, node.lineno, "exc_info", fmt))
        seen: set[str] = set()
        for argument in arguments:
            for _shape, leaf in [
                *_payload_leaves(argument),
                *_body_leaves(argument, bodies),
                *_whole_body_leaves(argument),
                *_expansion_leaves(argument),
                *_message_leaves(argument, messages),
                *(
                    leaf
                    for name in bound_names
                    if id(argument) not in traceback_args
                    for leaf in _exception_leaves(argument, name)
                ),
                *(
                    _retrieved_exception_leaves(argument)
                    if in_scope_for_i and id(argument) not in traceback_args
                    else []
                ),
                *(
                    _returned_exception_leaves(argument, carrying, bound_names)
                    if in_scope_for_i and id(argument) not in traceback_args
                    else []
                ),
            ]:
                if leaf in seen or leaf in rotating:
                    continue
                if _is_reviewed(reviewed, relative, leaf, fmt):
                    continue
                seen.add(leaf)
                offenders.append((relative, node.lineno, leaf, fmt))
    return offenders, scanned


def test_no_log_call_passes_raw_payload_bytes() -> None:
    offenders, scanned = scan()
    # Vacuum guard: the package carries well over 1400 log calls across all
    # levels; a scan that sees fewer did not walk the package (wrong root,
    # parse failure, renamed logger).
    assert scanned > 1400, (
        f"only {scanned} log calls scanned; the guard would be vacuous"
    )
    assert offenders == [], (
        "Log calls pass raw payload, token or key bytes (AGENTS.md section 5): "
        "log the length or a content class instead, or, if the value is an EID, "
        "name the leaf after it (`eid`, `eid_hex`, `truncated_eid_hex`); a reviewed "
        "rotating site goes into `_REVIEWED` as (path, leaf, format-string prefix):\n"
        + "\n".join(f"{path}:{line}: {leaf}" for path, line, leaf, _fmt in offenders)
    )


def test_shape_f_reports_every_unpacked_mapping() -> None:
    """Shape (F) names the mapping behind each `**` entry, wherever it sits.

    Positive fixture: the package is clean, so without this case a detector
    that returned nothing would keep `test_no_log_call_passes_raw_payload_bytes`
    green. The last snippet is the fixed record and must stay silent.
    """
    cases = {
        'LOG.debug("x: %s", {**gcm_data, "token": redacted})': ["gcm_data"],
        'LOG.debug("x: %s", repr({"inner": {**creds}}))': ["creds"],
        'LOG.debug("x: %s", {**a, "k": 1, **self.b})': ["a", "b"],
        'LOG.debug("x: %s", {"k": v})': [],
        'LOG.debug("x: fields=%s token=%s", sorted(gcm_data), redacted)': [],
    }
    for source, expected in cases.items():
        call = ast.parse(source).body[0].value
        assert isinstance(call, ast.Call)
        leaves = [
            leaf
            for argument in call.args
            for _shape, leaf in _expansion_leaves(argument)
        ]
        assert leaves == expected, source


def test_shape_d_follows_assignments_and_log_aliases() -> None:
    """Shape (D) taints through assignments; a bound log method is a log call.

    Positive fixture: `snippet` is bound from a helper that received the body,
    two hops after `content = await resp.read()`, in source order that puts
    the hop before the read (the fixpoint is order-independent). The log
    call goes through `log_fn`, bound from `_LOGGER.info`/`.warning`.
    """
    source = """
async def fetch(self, resp, first):
    snippet = _redact(_decode(content, 503))
    content = await resp.read()
    described = _describe_error_response(content, 503)
    size = len(content)
    log_fn = _LOGGER.info if first else _LOGGER.warning
    log_fn("a: %s", snippet)
    log_fn("b: %s %s", described, size)
    _LOGGER.error("c: %s", content)
    for line in content.splitlines():
        key, _, value = line.partition("=")
        code = value.strip().upper()
    _LOGGER.warning("d: %s %s", key, code)
    _LOGGER.warning("e: %s", _classify_error_code(value))
"""
    function = ast.parse(source).body[0]
    assert _body_names(function) == frozenset(
        {"content", "snippet", "line", "key", "_", "value", "code"}
    )
    assert _log_aliases(function) == frozenset({"log_fn"})
    offenders, scanned = _scan_tree(ast.parse(source), "fixture.py")
    assert scanned == 5
    assert [(line, leaf) for _p, line, leaf, _f in offenders] == [
        (8, "snippet"),
        (10, "content"),
        (14, "key"),
        (14, "code"),
    ]


def test_message_taint_reaches_text_and_producers_seed_bodies() -> None:
    """Local message bindings, their text, and library producers are followed.

    (G) taints through a constructor, an alias, a subscript and a `for` over
    a message field; `str(device)` turns that into a body (D); a nested
    function that calls a gpsoauth producer makes `resp` a body, and the
    error value derived from it stays a body through `.get()` and `[:32]`.
    """
    source = """
from custom_components.googlefindmy.ProtoDecoders import DeviceUpdate_pb2

async def decode(self, device_list: DeviceUpdate_pb2.DevicesList, raw):
    update = DeviceUpdate_pb2.DeviceUpdate.FromString(raw)
    alias = update
    for device in getattr(device_list, "deviceMetadata", []):
        names = [f.name for f, _ in device.ListFields()]
        device_str = str(device)
        unknown = [line for line in device_str.splitlines() if line[:1].isdigit()]
        _LOGGER.debug("a: %s %s", names, unknown)
        _LOGGER.debug("b: %s", _unknown_field_numbers(device))
    first = device_list.deviceMetadata[0]
    _LOGGER.debug("c: %s", first)
    code = alias.error.code
    _LOGGER.debug("d: %s", code)

async def exchange(self, username):
    def _run():
        return _gpsoauth().exchange_token(username)
    resp = await loop.run_in_executor(None, _run)
    error_value = resp.get("Error", "")
    kind = str(error_value)[:32]
    _LOGGER.warning("e: %s", kind, extra={"kind": kind, "n": len(resp)})
    _LOGGER.warning("f: %s", classify_gpsoauth_error(error_value))
"""
    tree = ast.parse(source)
    types = _message_type_names(tree)
    assert "DeviceUpdate_pb2" in types
    assert "pb" in _message_type_names(ast.parse("import pkg.sub.update_pb2 as pb"))
    assert "update_pb2" in _message_type_names(ast.parse("import pkg.update_pb2"))
    decode, exchange = tree.body[1], tree.body[2]
    assert _message_names(decode, types) == frozenset(
        {"device_list", "update", "alias", "device", "first"}
    )
    # `line` is a comprehension variable, not a binding the guard follows.
    assert _body_names(decode, _message_names(decode, types)) == frozenset(
        {"device_str", "unknown"}
    )
    assert _producer_names(exchange) == frozenset({"_run"})
    assert _body_names(exchange) == frozenset({"resp", "error_value", "kind"})
    offenders, scanned = _scan_tree(tree, "fixture.py")
    assert scanned == 6
    # Line numbers count from the leading newline of the fixture; the walk is
    # breadth-first, so the list is sorted before comparing.
    assert sorted((line, leaf) for _p, line, leaf, _f in offenders) == [
        (11, "unknown"),
        (14, "first"),
        (24, "kind"),
    ]


def test_shape_g_reports_message_parameters_passed_whole() -> None:
    """Shape (G) names a message-typed parameter passed whole, in any wrapping.

    Positive fixture with the same purpose as the shape (F) case. The module
    imports `HeartbeatAck` from a `_pb2` module and types `p` with the alias.
    """
    source = """
from .proto.mcs_pb2 import HeartbeatAck, DataMessageStanza as Stanza

async def handle(self, msg: HeartbeatAck, p: MessageProto | None, raw: bytes):
    LOG.debug("a: %s", msg)
    LOG.debug("b: %s", str(p))
    LOG.debug("c: %s", f"got {msg!r}")
    LOG.debug("d: %s", self._msg_str(msg))
    LOG.debug("e: %s %s", type(msg).__name__, msg.stream_id)
    LOG.debug("f: %s", raw)
"""
    tree = ast.parse(source)
    types = _message_type_names(tree)
    assert {"HeartbeatAck", "Stanza", "MessageProto"} <= types
    function = tree.body[1]
    messages = _message_params(function, types)
    assert messages == frozenset({"msg", "p"})
    calls = [node.value for node in function.body]
    reported = [
        leaf
        for call in calls
        for argument in call.args[1:]
        for _shape, leaf in _message_leaves(argument, messages)
    ]
    assert reported == ["msg", "p", "msg"]


def test_shape_h_reports_bodies_in_exception_messages() -> None:
    """Shape (H) scans exception constructors: direct and two-step raise.

    Fixture in the shape of the two gpsoauth sites this shape was added for
    (`token_retrieval.py:231`, `aas_token_retrieval.py:360`): the body is a
    producer result, the message is built with the body's field and raised
    directly, or bound first and raised two lines later. Described values
    pass.
    """
    source = """
def exchange(username):
    resp = gpsoauth.exchange_token(username)
    detail = str(resp.get("Error", "")).strip()
    if detail:
        raise InvalidAasTokenError(f"rejected: {detail}")
    new_err = RuntimeError(f"invalid: {resp}")
    raise new_err
    err = make_error(f"kept: {detail}")
    raise err
    LOG.warning("safe: %s", classify_gpsoauth_error(detail))
    raise RuntimeError(f"safe: {len(resp)} keys")
"""
    tree = ast.parse(source)
    offenders, scanned = _scan_tree(tree, "fixture.py")
    # Five sinks: the direct raise, `new_err = RuntimeError(...)`,
    # `err = make_error(...)` (raised later, so a constructor by use), the
    # warning and the described raise. `raise new_err` is a name, not a
    # call, and is not a sink of its own.
    assert scanned == 5
    assert sorted((line, leaf, fmt) for _p, line, leaf, fmt in offenders) == [
        (6, "detail", "raise"),
        (7, "resp", "raise"),
        (9, "detail", "raise"),
    ]


def test_each_reviewed_site_matches_exactly_one_log_call() -> None:
    """The exception list excuses one vetted call per entry, no more, no less.

    Fewer than one: the entry outlived the site it excuses. More than one: a
    log call was copied or moved and kept the vetted prefix, so raw bytes
    could be logged while this guard stays green.
    """
    offenders, _ = scan(reviewed=frozenset())
    matches = {
        site: [
            (path, line)
            for path, line, leaf, fmt in offenders
            if _is_reviewed(frozenset({site}), path, leaf, fmt)
        ]
        for site in _REVIEWED
    }
    wrong = {site: hits for site, hits in matches.items() if len(hits) != 1}
    assert wrong == {}, (
        "each reviewed site must match exactly one raw-bytes log call "
        f"(0 = stale entry, >1 = copied site): {wrong}"
    )


_SHAPE_I_FIXTURE = """
def exchange(user):
    try:
        return call()
    except Exception as exc:
        _LOGGER.error("failed: %s", exc)
        _LOGGER.error("failed: %s", _clip(exc))
        _LOGGER.error("failed: %s", str(exc))
        _LOGGER.error(f"failed: {exc}")
        _LOGGER.error("failed: %s", exc.args)
        _LOGGER.error("failed: %s", describe_exception(exc))
        _LOGGER.error("failed: %s", type(exc).__name__)
        _LOGGER.error("failed: %s", exc.errno)
        _LOGGER.error("failed", exc_info=exc)
        _LOGGER.error(f"{describe_exception(exc)} raw={exc}")
    except ValueError as builtin_type:
        _LOGGER.error("failed: %s", builtin_type)
    except OwnError as own_type:
        _LOGGER.error("failed: %s", own_type, exc_info=own_type)


def unnamed():
    last_error = None
    try:
        return call()
    except Exception as exc:
        last_error = exc
    _LOGGER.exception("failed")
    _LOGGER.error("failed", exc_info=True)
    _LOGGER.error("failed", exc_info=False)
    _LOGGER.error("failed", exc_info=None)
    _LOGGER.error("failed", exc_info=last_error)
    _LOGGER.error("failed", exc_info=(type(last_error), last_error, None))


class OwnError(Exception):
    pass
"""


_SHAPE_I_PARAMETER_FIXTURE = """
import logging
import cryptography.exceptions
import cryptography.exceptions as ce
from typing import Optional as Maybe
from collections.abc import Sequence as Seq
from aiohttp import ClientError
from aiohttp import ClientError as TransportFailure
from cryptography.exceptions import InvalidTag
from .exceptions import FatalRegistrationError

_LOGGER = logging.getLogger(__name__)


def _foreign(entry_id: str, err: ClientError) -> None:
    _LOGGER.error("[entry=%s] client error: %s", entry_id, err)


def _broad(entry_id: str, err: BaseException) -> None:
    _LOGGER.info("[entry=%s] failed: %s", entry_id, err)
    _LOGGER.info("[entry=%s] failed: %s", entry_id, describe_exception(err))


def _own(entry_id: str, err: FatalRegistrationError) -> None:
    _LOGGER.error("[entry=%s] fatal: %s", entry_id, err)


def _union(err: "OSError | ValueError") -> None:
    _LOGGER.debug("io: %s", err)


def _attribute(err: ssl.SSLError | None) -> None:
    _LOGGER.debug("tls: %s", err)


def _handler_without_name(err: BaseException) -> None:
    try:
        pass
    except Exception:
        _LOGGER.debug("still bound: %s", err)


def _optional(err: Optional[ClientError]) -> None:
    _LOGGER.debug("maybe: %s", err)


def _right_member(err: None | ClientError) -> None:
    _LOGGER.debug("right: %s", err)


def _kind(kind: type[Exception]) -> None:
    _LOGGER.debug("class, not instance: %s", kind)


def _second(first: BaseException, second: OSError) -> None:
    _LOGGER.error("second: %s", second)


def _both(err: BaseException) -> None:
    try:
        pass
    except OSError as inner:
        _LOGGER.error("both: %s %s", err, inner)


def _retrieved(task, err: BaseException) -> None:
    exc = task.exception()
    if exc:
        _LOGGER.error("task: %s %s", err, exc)


def _variadic(*errors: BaseException, **named: OSError) -> None:
    _LOGGER.error("many: %s %s", errors, named)


def _aliased(err: TransportFailure) -> None:
    _LOGGER.error("alias: %s", err)


def _annotated(task) -> None:
    exc: BaseException | None = task.exception()
    if exc:
        _LOGGER.error("typed: %s", exc)


def _walrus(task) -> None:
    if (exc := task.exception()):
        _LOGGER.error("walrus: %s", exc)


def _direct(task) -> None:
    _LOGGER.error("direct: %s %s", task.exception(), describe_exception(task.exception()))


def _attribute_target(self, task) -> None:
    self._last = task.exception()
    _LOGGER.error("attr: %s", self._last)


def _container(errors: list[ClientError], pairs: tuple[OSError, ...]) -> None:
    _LOGGER.error("many: %s %s", errors, pairs)


def _no_suffix(group: ExceptionGroup, stop: SystemExit) -> None:
    _LOGGER.error("group: %s %s", group, stop)


def _mapping(errors: dict[str, BaseException], keys: dict[OSError, str]) -> None:
    _LOGGER.error("map: %s %s", errors, keys)


def _heads(group: ExceptionGroup[OSError], tagged: Annotated[ClientError, "x"]) -> None:
    _LOGGER.error("heads: %s %s", group, tagged)


def _no_convention(err: InvalidTag) -> None:
    _LOGGER.error("tag: %s", err)


def _qualified(
    err: cryptography.exceptions.InvalidSignature, other: ce.AlreadyFinalized
) -> None:
    _LOGGER.error("qualified: %s %s", err, other)


def _carrier_alias(err: Maybe[ClientError], many: Seq[OSError]) -> None:
    _LOGGER.error("carrier alias: %s %s", err, many)


def _stdlib(errors: defaultdict[str, OSError], queue: deque[ClientError], pending: Future[OSError]) -> None:
    _LOGGER.error("stdlib: %s %s %s", errors, queue, pending)
"""


def test_shape_i_reports_annotated_exception_parameters() -> None:
    """A foreign exception bound outside any `except` is a sink.

    `_classify_registration_exception(entry_id, err: BaseException)` in
    `fcm_receiver_ha.py` logged the producer's text without any `except`
    in sight; the handler-only binding did not see it (2026-09-18). Reported:
    a library type (line 16), a broad type (line 20), a string union (line
    29), an attribute in a union with `None` (line 33, `ssl.SSLError` is
    read like `_is_foreign_handler` reads it), a parameter logged inside a
    handler without `as` (line 40, the handler does not clear the binding),
    `Optional[...]` (line 44), a union whose foreign member is on the right
    (line 48), the second of two annotated parameters (line 56, every bound
    name is tracked, not the first), a parameter next to an `as` handler
    (line 63, both names, the handler accumulates, it does not replace), a
    name assigned from `task.exception()` next to a parameter (line 69,
    `err` and `exc`, the shape of `_track_task._done`), variadic parameters
    (line 73, `*errors` and `**named`), an import alias without a suffix
    (line 77, `TransportFailure` is `ClientError`) and an annotated
    assignment from `task.exception()` (line 83), a walrus (line 88), the
    call passed directly (line 92, leaf `exception()`; the wrapped second
    argument on the same line is not reported), an attribute target (line
    97, `self._last`) and container annotations (line 101, `list[...]` and
    `tuple[..., ...]`) and built-in types without a suffix (line 105,
    `ExceptionGroup` and `SystemExit`, read from `builtins`), a mapping
    whose values or keys are exceptions (line 109, `errors` and `keys`; a
    dict renders both), a generic exception head (`ExceptionGroup[OSError]`)
    and `Annotated[...]` (line 113), and an imported third-party type without
    a suffix, resolved as a class at import time (line 117, `InvalidTag`), and types
    reachable only module-qualified or through a module alias (line 123,
    `InvalidSignature` and `AlreadyFinalized`, neither imported by name), and
    carriers imported under an alias (line 127, `Maybe[...]`, `Seq[...]`),
    and the stdlib containers and an `asyncio` future (line 131,
    `defaultdict[str, OSError]`, `deque[...]`, `Future[...]`; Codex review
    on 714596f). Not reported:
    the wrapped
    argument (line 21), an own type (line 25), which carries text this
    package wrote, and `type[Exception]` (line 52, the only parameter of its
    function, so its absence from the list is a measurement, not a vacuum),
    which is a class, not an instance.
    """
    tree = ast.parse(_SHAPE_I_PARAMETER_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    by_line = sorted((line, leaf) for _, line, leaf, _ in offenders)
    assert by_line == [
        (16, "err"),
        (20, "err"),
        (29, "err"),
        (33, "err"),
        (40, "err"),
        (44, "err"),
        (48, "err"),
        (56, "second"),
        (63, "err"),
        (63, "inner"),
        (69, "err"),
        (69, "exc"),
        (73, "errors"),
        (73, "named"),
        (77, "err"),
        (83, "exc"),
        (88, "exc"),
        (92, "exception()"),
        (97, "self._last"),
        (101, "errors"),
        (101, "pairs"),
        (105, "group"),
        (105, "stop"),
        (109, "errors"),
        (109, "keys"),
        (113, "group"),
        (113, "tagged"),
        (117, "err"),
        (123, "err"),
        (123, "other"),
        (127, "err"),
        (127, "many"),
        (131, "errors"),
        (131, "pending"),
        (131, "queue"),
    ]
    outside, _ = _scan_tree(tree, "coordinator/fixture.py")
    assert outside == []


# A fixture of its own, not an addition to the one above: the pin `35` there is
# the promise "in this bounded set, shape (I) reports exactly these sites". After
# an addition, a regression among those 35 could no longer be told apart from a
# change to the new cases.
_SHAPE_I_ALIAS_FIXTURE = """
import logging
import cryptography.exceptions as cex
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, Any, NewType, Optional, TypeAlias, TypeVar
from typing import TypeAlias as TA
from aiohttp import ClientError
from aiohttp import ClientError as TransportFailure
from ._typing import TransportError
from .exceptions import FatalRegistrationError

_LOGGER = logging.getLogger(__name__)


type AliasDirect = ClientError


def _alias_direct(err: AliasDirect) -> None:
    _LOGGER.error("F-01 direct: %s", err)  # I:err


type AliasUnion = ClientError | OSError


def _alias_union(err: AliasUnion) -> None:
    _LOGGER.error("F-02 union: %s", err)  # I:err


type AliasContainer = list[ClientError]


def _alias_container(err: AliasContainer) -> None:
    _LOGGER.error("F-03 container: %s", err)  # I:err


type AliasOptional = Optional[ClientError]


def _alias_optional(err: AliasOptional) -> None:
    _LOGGER.error("F-04 optional: %s", err)  # I:err


type AliasHop1 = ClientError
type AliasHop2 = AliasHop1


def _alias_chain(err: AliasHop2) -> None:
    _LOGGER.error("F-05 chain: %s", err)  # I:err


def _alias_defined_later(err: AliasLater) -> None:
    _LOGGER.error("F-06 later: %s", err)  # I:err


type AliasRecursive = ClientError | list[AliasRecursive]


def _alias_recursive(err: AliasRecursive) -> None:
    _LOGGER.error("F-07 recursive: %s", err)  # I:err


type AliasGenericValue[T] = ClientError


def _alias_generic_value(err: AliasGenericValue[int]) -> None:
    _LOGGER.error("F-08 generic value: %s", err)  # I:err


type AliasBox[T] = list[T]


def _alias_generic_argument(err: AliasBox[ClientError]) -> None:
    _LOGGER.error("F-09 generic argument: %s", err)  # I:err


type AliasInString = ClientError


def _alias_in_string(err: "AliasInString") -> None:
    _LOGGER.error("F-10 in string: %s", err)  # I:err


type AliasStringValue = "ClientError"


def _alias_string_value(err: AliasStringValue) -> None:
    _LOGGER.error("F-11 string value: %s", err)  # I:err


AliasConventional: TypeAlias = ClientError


def _alias_conventional(err: AliasConventional) -> None:
    _LOGGER.error("F-12 conventional: %s", err)  # I:err


AliasConventionalTA: TA = ClientError


def _alias_conventional_aliased(err: AliasConventionalTA) -> None:
    _LOGGER.error("F-13 conventional aliased: %s", err)  # I:err


AliasBare = ClientError


def _alias_bare(err: AliasBare) -> None:
    _LOGGER.error("F-14 bare: %s", err)  # I:err


type AliasHead = list


def _alias_as_head(err: AliasHead[ClientError]) -> None:
    _LOGGER.error("F-15 as head: %s", err)  # I:err


type AliasNested = ClientError


def _alias_nested(err: list[AliasNested]) -> None:
    _LOGGER.error("F-16 nested: %s", err)  # I:err


type AliasAnnotated = Annotated[ClientError, "x"]


def _alias_annotated(err: AliasAnnotated) -> None:
    _LOGGER.error("F-17 annotated: %s", err)  # I:err


type AliasQualified = cex.InvalidTag


def _alias_qualified(err: AliasQualified) -> None:
    _LOGGER.error("F-18 qualified: %s", err)  # I:err


type AliasOfImport = TransportFailure


def _alias_of_import_alias(err: AliasOfImport) -> None:
    _LOGGER.error("F-19 of import alias: %s", err)  # I:err


type AliasVariadic = ClientError


def _alias_variadic(*errors: AliasVariadic) -> None:
    _LOGGER.error("F-20 variadic: %s", errors)  # I:errors


type AliasMatch = ClientError


def _alias_match(value) -> None:
    match value:
        case AliasMatch() as caught:
            _LOGGER.error("F-21 match: %s", caught)  # I:caught


def _defines_inner_alias() -> None:
    type AliasInner = ClientError


def _alias_function_scope(err: AliasInner) -> None:
    _LOGGER.error("F-22 function scope: %s", err)  # I:err


class _AliasHolder:
    type AliasInClass = ClientError


def _alias_class_scope(err: AliasInClass) -> None:
    _LOGGER.error("F-23 class scope: %s", err)  # I:err


if TYPE_CHECKING:
    type AliasDup = str
else:
    type AliasDup = ClientError


def _alias_defined_twice(err: AliasDup) -> None:
    _LOGGER.error("F-24 twice: %s", err)  # I:err


type AliasHandled = ClientError


def _alias_handler() -> None:
    try:
        pass
    except AliasHandled as err:
        _LOGGER.error("F-25 handler: %s", err)  # I:err


type AliasHarmlessError = str


def _alias_keeps_suffix(err: AliasHarmlessError) -> None:
    _LOGGER.error("F-26 keeps suffix: %s", err)  # I:err


type TimeoutError = str


def _alias_shadows_builtin(err: TimeoutError) -> None:
    _LOGGER.error("F-27 shadows builtin: %s", err)  # I:err


type AliasOwn = FatalRegistrationError


def _alias_own_type(err: AliasOwn, companion: ClientError) -> None:
    _LOGGER.error("F-28 own: %s", err)  # S
    _LOGGER.error("F-28 companion: %s", companion)  # C:companion


type AliasKls = type[ClientError]


def _alias_class_object(err: AliasKls, companion: ClientError) -> None:
    _LOGGER.error("F-29 class object: %s", err)  # S
    _LOGGER.error("F-29 companion: %s", companion)  # C:companion


type AliasCb = Callable[..., ClientError]


def _alias_callable(err: AliasCb, companion: ClientError) -> None:
    _LOGGER.error("F-30 callable: %s", err)  # S
    _LOGGER.error("F-30 companion: %s", companion)  # C:companion


AliasT = TypeVar("AliasT", bound=BaseException)
type AliasTv = AliasT


def _alias_typevar(err: AliasTv, companion: ClientError) -> None:
    _LOGGER.error("F-31 typevar: %s", err)  # S
    _LOGGER.error("F-31 companion: %s", companion)  # C:companion


type AliasC1 = AliasC2
type AliasC2 = AliasC1


def _alias_cycle(err: AliasC1, companion: ClientError) -> None:
    _LOGGER.error("F-32 cycle: %s", err)  # S
    _LOGGER.error("F-32 companion: %s", companion)  # C:companion


def _alias_cross_module(err: TransportError, companion: ClientError) -> None:
    _LOGGER.error("F-33 cross module: %s", err)  # S
    _LOGGER.error("F-33 companion: %s", companion)  # C:companion


type AliasMade = NewType("AliasMade", ClientError)


def _alias_call_value(err: AliasMade, companion: ClientError) -> None:
    _LOGGER.error("F-34 call value: %s", err)  # S
    _LOGGER.error("F-34 companion: %s", companion)  # C:companion


def _alias_function_type_param[T: ClientError](err: T) -> None:
    _LOGGER.error("F-35 function type param: %s", err)  # I:err


async def _alias_async_type_param[T: ClientError](err: T) -> None:
    _LOGGER.error("F-35 async type param: %s", err)  # I:err


type AliasBroken = "!!"


def _alias_broken_string(err: AliasBroken, companion: ClientError) -> None:
    _LOGGER.error("F-36 broken string: %s", err)  # S
    _LOGGER.error("F-36 companion: %s", companion)  # C:companion


AliasMultiA = AliasMultiB = ClientError


def _alias_multi_target(err: AliasMultiB) -> None:
    _LOGGER.error("F-37 multi target: %s", err)  # I:err


AliasTupleA, AliasTupleB = ClientError, OSError


def _alias_tuple_target(err: AliasTupleB) -> None:
    _LOGGER.error("F-38 tuple target: %s", err)  # I:err


type AliasBound[T: ClientError] = list[T]


def _alias_head_bound(err: AliasBound[int]) -> None:
    _LOGGER.error("F-39 head bound: %s", err)  # I:err


type AliasDefault[T = ClientError] = list[T]


def _alias_head_default(err: AliasDefault[int]) -> None:
    _LOGGER.error("F-40 head default: %s", err)  # I:err


def _alias_class_qualified(err: _AliasHolder.AliasInClass) -> None:
    _LOGGER.error("F-41 class qualified: %s", err)  # I:err


type AliasAnnUse = ClientError


def _alias_annassign_use(companion: ClientError) -> None:
    err: AliasAnnUse = make()
    _LOGGER.error("F-42 annassign: %s", err)  # S
    _LOGGER.error("F-42 companion: %s", companion)  # C:companion


type AliasHeadType = type


def _alias_head_type(err: AliasHeadType[ClientError], companion: ClientError) -> None:
    _LOGGER.error("F-43 head type: %s", err)  # S
    _LOGGER.error("F-43 companion: %s", companion)  # C:companion


type AliasHeadCallable = Callable


def _alias_head_callable(
    err: AliasHeadCallable[..., ClientError], companion: ClientError
) -> None:
    _LOGGER.error("F-44 head callable: %s", err)  # S
    _LOGGER.error("F-44 companion: %s", companion)  # C:companion


type AliasW0 = ClientError
type AliasW1 = AliasW0 | AliasW0
type AliasW2 = AliasW1 | AliasW1
type AliasW3 = AliasW2 | AliasW2
type AliasW4 = AliasW3 | AliasW3
type AliasW5 = AliasW4 | AliasW4
type AliasW6 = AliasW5 | AliasW5
type AliasW7 = AliasW6 | AliasW6
type AliasW8 = AliasW7 | AliasW7
type AliasW9 = AliasW8 | AliasW8
type AliasW10 = AliasW9 | AliasW9
type AliasW11 = AliasW10 | AliasW10
type AliasW12 = AliasW11 | AliasW11
type AliasW13 = AliasW12 | AliasW12
type AliasW14 = AliasW13 | AliasW13
type AliasW15 = AliasW14 | AliasW14
type AliasW16 = AliasW15 | AliasW15
type AliasW17 = AliasW16 | AliasW16
type AliasW18 = AliasW17 | AliasW17
type AliasW19 = AliasW18 | AliasW18
type AliasW20 = AliasW19 | AliasW19


def _alias_doubling_chain(err: AliasW20) -> None:
    _LOGGER.error("F-45 doubling chain: %s", err)  # I:err


_alias_proxy = None
alias_gpsoauth = _alias_proxy


def _alias_value_binding(err: alias_gpsoauth, companion: ClientError) -> None:
    _LOGGER.error("F-46 value binding: %s", err)  # S
    _LOGGER.error("F-46 companion: %s", companion)  # C:companion


AliasCacheState = dict[str, Any]


def _alias_typeless_value(err: AliasCacheState, companion: ClientError) -> None:
    _LOGGER.error("F-47 typeless value: %s", err)  # S
    _LOGGER.error("F-47 companion: %s", companion)  # C:companion


type AliasHeadAnnotated = Annotated


def _alias_head_annotated(err: AliasHeadAnnotated[ClientError, "x"]) -> None:
    _LOGGER.error("F-48 head annotated: %s", err)  # I:err


class AliasMyCounter:
    pass


Counter = AliasMyCounter


def _alias_shadows_carrier(err: Counter[ClientError]) -> None:
    _LOGGER.error("F-49 shadows carrier: %s", err)  # I:err


AliasD0 = ClientError
AliasD1 = AliasD0
AliasD2 = AliasD1
AliasD3 = AliasD2
AliasD4 = AliasD3
AliasD5 = AliasD4
AliasD6 = AliasD5
AliasD7 = AliasD6
AliasD8 = AliasD7
AliasD9 = AliasD8
AliasD10 = AliasD9
AliasD11 = AliasD10
AliasD12 = AliasD11
AliasD13 = AliasD12
AliasD14 = AliasD13
AliasD15 = AliasD14
AliasD16 = AliasD15
AliasD17 = AliasD16
AliasD18 = AliasD17
AliasD19 = AliasD18
AliasD20 = AliasD19
AliasD21 = AliasD20
AliasD22 = AliasD21
AliasD23 = AliasD22
AliasD24 = AliasD23
AliasD25 = AliasD24
AliasD26 = AliasD25
AliasD27 = AliasD26
AliasD28 = AliasD27
AliasD29 = AliasD28
AliasD30 = AliasD29
AliasD31 = AliasD30
AliasD32 = AliasD31
AliasD33 = AliasD32
AliasD34 = AliasD33
AliasD35 = AliasD34
AliasD36 = AliasD35
AliasD37 = AliasD36
AliasD38 = AliasD37
AliasD39 = AliasD38
AliasD40 = AliasD39
AliasD41 = AliasD40
AliasD42 = AliasD41
AliasD43 = AliasD42
AliasD44 = AliasD43
AliasD45 = AliasD44
AliasD46 = AliasD45
AliasD47 = AliasD46
AliasD48 = AliasD47
AliasD49 = AliasD48
AliasD50 = AliasD49
AliasD51 = AliasD50
AliasD52 = AliasD51
AliasD53 = AliasD52
AliasD54 = AliasD53
AliasD55 = AliasD54
AliasD56 = AliasD55
AliasD57 = AliasD56
AliasD58 = AliasD57
AliasD59 = AliasD58
AliasD60 = AliasD59
AliasD61 = AliasD60
AliasD62 = AliasD61
AliasD63 = AliasD62
AliasD64 = AliasD63
AliasD65 = AliasD64
AliasD66 = AliasD65
AliasD67 = AliasD66
AliasD68 = AliasD67
AliasD69 = AliasD68
AliasD70 = AliasD69


def _alias_deep_chain(err: AliasD70) -> None:
    _LOGGER.error("F-50 deep chain: %s", err)  # I:err


type AliasLater = ClientError


HarmD0 = str
HarmD1 = HarmD0
HarmD2 = HarmD1
HarmD3 = HarmD2
HarmD4 = HarmD3
HarmD5 = HarmD4
HarmD6 = HarmD5
HarmD7 = HarmD6
HarmD8 = HarmD7
HarmD9 = HarmD8
HarmD10 = HarmD9
HarmD11 = HarmD10
HarmD12 = HarmD11
HarmD13 = HarmD12
HarmD14 = HarmD13
HarmD15 = HarmD14
HarmD16 = HarmD15
HarmD17 = HarmD16
HarmD18 = HarmD17
HarmD19 = HarmD18
HarmD20 = HarmD19
HarmD21 = HarmD20
HarmD22 = HarmD21
HarmD23 = HarmD22
HarmD24 = HarmD23
HarmD25 = HarmD24
HarmD26 = HarmD25
HarmD27 = HarmD26
HarmD28 = HarmD27
HarmD29 = HarmD28
HarmD30 = HarmD29
HarmD31 = HarmD30
HarmD32 = HarmD31
HarmD33 = HarmD32
HarmD34 = HarmD33
HarmD35 = HarmD34
HarmD36 = HarmD35
HarmD37 = HarmD36
HarmD38 = HarmD37
HarmD39 = HarmD38
HarmD40 = HarmD39
HarmD41 = HarmD40
HarmD42 = HarmD41
HarmD43 = HarmD42
HarmD44 = HarmD43
HarmD45 = HarmD44
HarmD46 = HarmD45
HarmD47 = HarmD46
HarmD48 = HarmD47
HarmD49 = HarmD48
HarmD50 = HarmD49
HarmD51 = HarmD50
HarmD52 = HarmD51
HarmD53 = HarmD52
HarmD54 = HarmD53
HarmD55 = HarmD54
HarmD56 = HarmD55
HarmD57 = HarmD56
HarmD58 = HarmD57
HarmD59 = HarmD58
HarmD60 = HarmD59
HarmD61 = HarmD60
HarmD62 = HarmD61
HarmD63 = HarmD62
HarmD64 = HarmD63
HarmD65 = HarmD64
HarmD66 = HarmD65
HarmD67 = HarmD66
HarmD68 = HarmD67
HarmD69 = HarmD68
HarmD70 = HarmD69


def _alias_deep_chain_harmless(err: HarmD70) -> None:
    _LOGGER.error("F-51 deep chain harmless: %s", err)  # I:err


type AliasAnnSecond = Annotated[str, ClientError]


def _alias_annotated_second(err: AliasAnnSecond, companion: ClientError) -> None:
    _LOGGER.error("F-52 annotated second: %s", err)  # S
    _LOGGER.error("F-52 companion: %s", companion)  # C:companion


if TYPE_CHECKING:
    type AliasDupLast = ClientError
else:
    type AliasDupLast = str


def _alias_defined_twice_harmless_last(err: AliasDupLast) -> None:
    _LOGGER.error("F-53 twice harmless last: %s", err)  # I:err


type AliasHd0 = list
type AliasHd1 = AliasHd0


def _alias_head_two_hops(err: AliasHd1[ClientError]) -> None:
    _LOGGER.error("F-54 head two hops: %s", err)  # I:err


type AliasNestHead = dict


def _alias_head_nested(err: AliasNestHead[str, AliasNestHead[str, ClientError]]) -> None:
    _LOGGER.error("F-55 head nested: %s", err)  # I:err


def _alias_unbounded_param[T](err: T, companion: ClientError) -> None:
    _LOGGER.error("F-56 unbounded param: %s", err)  # S
    _LOGGER.error("F-56 companion: %s", companion)  # C:companion


AliasPlainString = "ClientError"


def _alias_plain_string(err: AliasPlainString) -> None:
    _LOGGER.error("F-57 plain string: %s", err)  # I:err


*AliasStarRest, AliasStarLast = str, int, ClientError


def _alias_starred_target(err: AliasStarLast) -> None:
    _LOGGER.error("F-58 starred target: %s", err)  # I:err


(AliasNestA, AliasNestB), AliasNestC = (ClientError, OSError), ValueError


def _alias_nested_target(err: AliasNestA) -> None:
    _LOGGER.error("F-59 nested target: %s", err)  # I:err


type HarmW0 = str
type HarmW1 = HarmW0 | HarmW0
type HarmW2 = HarmW1 | HarmW1
type HarmW3 = HarmW2 | HarmW2
type HarmW4 = HarmW3 | HarmW3
type HarmW5 = HarmW4 | HarmW4
type HarmW6 = HarmW5 | HarmW5
type HarmW7 = HarmW6 | HarmW6
type HarmW8 = HarmW7 | HarmW7
type HarmW9 = HarmW8 | HarmW8
type HarmW10 = HarmW9 | HarmW9


def _alias_doubling_chain_harmless(err: HarmW10) -> None:
    _LOGGER.error("F-60 doubling chain harmless: %s", err)  # I:err


type AliasUnusedBound[T: ClientError] = list[int]


def _alias_unused_bound(err: AliasUnusedBound[int], companion: ClientError) -> None:
    _LOGGER.error("F-61 unused bound: %s", err)  # S
    _LOGGER.error("F-61 companion: %s", companion)  # C:companion


def _alias_outer_bound[T: ClientError]() -> None:
    def _inner(err: T) -> None:
        _LOGGER.error("F-62 nested function: %s", err)  # I:err

    _inner


type AliasBoundCallable[T: ClientError] = Callable[[T], None]


def _alias_bound_in_callable(err: AliasBoundCallable, companion: ClientError) -> None:
    _LOGGER.error("F-63 bound in callable: %s", err)  # S
    _LOGGER.error("F-63 companion: %s", companion)  # C:companion


type AliasBoundStrSlot[T: ClientError] = list["T"]


def _alias_bound_in_string_slot(err: AliasBoundStrSlot) -> None:
    _LOGGER.error("F-64 bound in string slot: %s", err)  # I:err


type AliasDefaultStrValue[T = ClientError] = "list[T]"


def _alias_default_in_string_value(err: AliasDefaultStrValue) -> None:
    _LOGGER.error("F-65 default in string value: %s", err)  # I:err


def _alias_outer_shadowed[T: ClientError]() -> None:
    def _inner[T](err: T, companion: ClientError) -> None:
        _LOGGER.error("F-66 shadowed by function: %s", err)  # S
        _LOGGER.error("F-66 companion: %s", companion)  # C:companion

    _inner


def _alias_outer_class_shadowed[T: ClientError]() -> None:
    class _Holder[T]:
        def method(self, err: T, companion: ClientError) -> None:
            _LOGGER.error("F-67 shadowed by class: %s", err)  # S
            _LOGGER.error("F-67 companion: %s", companion)  # C:companion

    _Holder


type AliasMaybe[T] = T | None


def _alias_generic_arg_union(err: AliasMaybe[ClientError]) -> None:
    _LOGGER.error("F-69 generic argument in a union: %s", err)  # I:err


type AliasId[T] = T


def _alias_generic_arg_bare(err: AliasId[ClientError]) -> None:
    _LOGGER.error("F-70 generic argument as the value: %s", err)  # I:err


type AliasFn[T] = Callable[[T], None]


def _alias_generic_arg_callable(err: AliasFn[ClientError], companion: ClientError) -> None:
    _LOGGER.error("F-71 generic argument in a callable: %s", err)  # S
    _LOGGER.error("F-71 companion: %s", companion)  # C:companion


type AliasMaybeStr[T] = "T | None"


def _alias_generic_arg_string_value(err: AliasMaybeStr[ClientError]) -> None:
    _LOGGER.error("F-72 generic argument in a string value: %s", err)  # I:err


def _alias_generic_arg_string(err: AliasId["ClientError"]) -> None:
    _LOGGER.error("F-73 generic argument as a string: %s", err)  # I:err


type AliasSecondHop[T: ClientError, U = T] = list[U]


def _alias_default_second_hop(err: AliasSecondHop) -> None:
    _LOGGER.error("F-74 default naming an earlier parameter: %s", err)  # I:err


type AliasPair[T, U = T] = U | None


def _alias_default_from_argument(err: AliasPair[ClientError]) -> None:
    _LOGGER.error("F-75 default following an argument: %s", err)  # I:err


def _alias_generic_unbound(err: AliasId, companion: ClientError) -> None:
    _LOGGER.error("F-76 generic alias without argument: %s", err)  # S
    _LOGGER.error("F-76 companion: %s", companion)  # C:companion


type AliasConstraints[T: (ClientError, int)] = T


def _alias_constraints_bare(err: AliasConstraints) -> None:
    _LOGGER.error("F-77 constraint tuple as the value: %s", err)  # I:err


def _function_constraints[T: (ClientError, int)](err: T) -> None:
    _LOGGER.error("F-78 constraint tuple of a function: %s", err)  # I:err


type AliasConstraintsMap[T: (ClientError, int)] = dict[str, T]


def _alias_constraints_mapping(err: AliasConstraintsMap) -> None:
    _LOGGER.error("F-79 constraint tuple in a mapping: %s", err)  # I:err


type AliasOwnParam[T] = list[T]


def _alias_param_vs_function_param[T: ClientError]() -> None:
    def _inner(err: AliasOwnParam, companion: ClientError) -> None:
        _LOGGER.error("F-80 alias parameter is not the caller's: %s", err)  # S
        _LOGGER.error("F-80 companion: %s", companion)  # C:companion

    _inner


def _function_param_as_argument[T: ClientError](err: AliasMaybe[T]) -> None:
    _LOGGER.error("F-81 bounded parameter as argument: %s", err)  # I:err


Shadowed: TypeAlias = ClientError


def _function_param_shadows_alias[Shadowed: int]() -> None:
    def _inner(err: Shadowed, companion: ClientError) -> None:
        _LOGGER.error("F-82 parameter shadows a module alias: %s", err)  # S
        _LOGGER.error("F-82 companion: %s", companion)  # C:companion

    _inner


def _function_body_rebinds_param[Rebound: int]() -> None:
    Rebound = ClientError

    def _inner(err: Rebound) -> None:
        _LOGGER.error("F-83 body rebinds the parameter: %s", err)  # I:err

    _inner


class _BoundHolder[T: ClientError]:
    def method(self, err: T) -> None:
        _LOGGER.error("F-84 class parameter bound: %s", err)  # I:err


class _ShadowHolder[Shadowed]:
    def method(self, err: Shadowed, companion: ClientError) -> None:
        _LOGGER.error("F-85 class parameter shadows a module alias: %s", err)  # S
        _LOGGER.error("F-85 companion: %s", companion)  # C:companion


type AliasOuter[U] = AliasMaybe[U]


def _alias_generic_in_generic(err: AliasOuter[ClientError]) -> None:
    _LOGGER.error("F-86 argument through two generic aliases: %s", err)  # I:err


type AliasTup[*Ts] = tuple[*Ts]


def _alias_type_var_tuple(err: AliasTup[int, ClientError]) -> None:
    _LOGGER.error("F-87 type variable tuple: %s", err)  # I:err


def _starred_in_tuple(err: tuple[*tuple[ClientError]]) -> None:
    _LOGGER.error("F-88 starred slot: %s", err)  # I:err


# `Unpack` from `typing`, not imported here so that no line above moves.
def _unpack_in_tuple(err: tuple[Unpack[tuple[ClientError]]]) -> None:
    _LOGGER.error("F-89 unpacked slot: %s", err)  # I:err


def _alias_surplus_argument(err: AliasId[int, ClientError]) -> None:
    _LOGGER.error("F-92 surplus argument: %s", err)  # I:err


type AliasBoundArg[T: ClientError] = list[T]


def _alias_bound_with_argument(err: AliasBoundArg[str]) -> None:
    _LOGGER.error("F-93 bound beside an argument: %s", err)  # I:err


type AliasDefaultArg[T = ClientError] = list[T]


def _alias_default_beside_argument(err: AliasDefaultArg[str]) -> None:
    _LOGGER.error("F-94 default beside an argument: %s", err)  # I:err


def _function_bound_and_default[T: int = ClientError](err: T) -> None:
    _LOGGER.error("F-95 bound and default: %s", err)  # I:err


def _function_param_and_module_alias[T: int](err: AliasDirect) -> None:
    _LOGGER.error("F-96 module alias beside a type parameter: %s", err)  # I:err


type AliasParamSpec[**P, R] = Callable[P, R]


def _alias_param_spec(err: AliasParamSpec[[ClientError], None], companion: ClientError) -> None:
    _LOGGER.error("F-97 parameter specification: %s", err)  # S
    _LOGGER.error("F-97 companion: %s", companion)  # C:companion


type AliasAnnFirst[T] = Annotated[T, "meta"]


def _alias_annotated_first_slot(err: AliasAnnFirst[ClientError]) -> None:
    _LOGGER.error("F-98 argument in the first slot of Annotated: %s", err)  # I:err


type AliasAnnMeta[T] = Annotated[int, T]


def _alias_annotated_metadata(err: AliasAnnMeta[ClientError], companion: ClientError) -> None:
    _LOGGER.error("F-99 argument in the metadata of Annotated: %s", err)  # S
    _LOGGER.error("F-99 companion: %s", companion)  # C:companion


type AliasMid[A, *Ts, B] = A | B


def _alias_type_var_tuple_middle(
    err: AliasMid[int, ClientError, ClientError, int], companion: ClientError
) -> None:
    _LOGGER.error("F-100 type variable tuple takes the middle: %s", err)  # S
    _LOGGER.error("F-100 companion: %s", companion)  # C:companion


type AliasHook[**P] = Callable[P, None]


def _alias_sole_param_spec(err: AliasHook[int, ClientError], companion: ClientError) -> None:
    _LOGGER.error("F-101 sole parameter specification: %s", err)  # S
    _LOGGER.error("F-101 companion: %s", companion)  # C:companion


Tv = TypeVar("Tv")


def _bounded_elsewhere[Tv: ClientError](x: Tv) -> None:
    pass


def _undeclared_type_var(err: Tv, companion: ClientError) -> None:
    _LOGGER.error("F-102 a bound stays in its own function: %s", err)  # S
    _LOGGER.error("F-102 companion: %s", companion)  # C:companion


type AliasNestL[T] = list[T]


def _alias_generic_nested_same(err: AliasNestL[AliasNestL[ClientError]]) -> None:
    _LOGGER.error("F-103 the same generic alias in its argument: %s", err)  # I:err


type AliasNestM[K, V] = dict[K, V]


def _alias_generic_nested_mapping(err: AliasNestM[str, AliasNestM[str, ClientError]]) -> None:
    _LOGGER.error("F-104 the same over two parameters: %s", err)  # I:err


def _alias_generic_nested_bare(err: AliasId[AliasId[ClientError]]) -> None:
    _LOGGER.error("F-105 a bare parameter, nested: %s", err)  # I:err


AliasOptArg = AliasMaybe[ClientError]


def _alias_generic_via_plain_alias(err: AliasMaybe[AliasOptArg]) -> None:
    _LOGGER.error("F-106 a plain alias on the same generic alias: %s", err)  # I:err


def _alias_generic_nested_surplus(err: AliasId[int, AliasId[ClientError]]) -> None:
    _LOGGER.error("F-107 a surplus argument with the same alias: %s", err)  # I:err


type AliasRes[V, TError] = V | TError


def _alias_param_named_like_error(err: AliasRes[int, str], companion: ClientError) -> None:
    _LOGGER.error("F-108 a bound parameter's name is no member: %s", err)  # S
    _LOGGER.error("F-108 companion: %s", companion)  # C:companion


type AliasDefaultDoubling[T1 = "", T2 = T1 | T1, T3 = T2 | T2, T4 = T3 | T3, T5 = T4 | T4, T6 = T5 | T5, T7 = T6 | T6, T8 = T7 | T7, T9 = T8 | T8, T10 = T9 | T9, T11 = T10 | T10, T12 = T11 | T11, T13 = T12 | T12, T14 = T13 | T13, T15 = T14 | T14, T16 = T15 | T15, T17 = T16 | T16, T18 = T17 | T17, T19 = T18 | T18, T20 = T19 | T19, T21 = T20 | T20, T22 = T21 | T21] = T22


def _alias_default_doubling_unreadable(err: AliasDefaultDoubling) -> None:
    _LOGGER.error("F-109 doubling defaults on an unreadable leaf: %s", err)  # I:err


type AliasSelfDefault[T = AliasSelfDefault[int]] = T


def _alias_self_default(err: AliasSelfDefault, companion: ClientError) -> None:
    _LOGGER.error("F-110 a default that names its own alias: %s", err)  # S
    _LOGGER.error("F-110 companion: %s", companion)  # C:companion


type AliasSelfBound[T: AliasSelfBound[int]] = T


def _alias_self_bound(err: AliasSelfBound, companion: ClientError) -> None:
    _LOGGER.error("F-111 a bound that names its own alias: %s", err)  # S
    _LOGGER.error("F-111 companion: %s", companion)  # C:companion


# This module binds the name `BaseException` itself. Without the identity check
# in `_is_foreign_exception_annotation`, the `own` rule would swallow the
# fail-closed member of a truncated expansion and F-50 and F-51 would go silent.
# It sits last so no line number above it moves.
class BaseException(Exception):
    pass
"""


def test_shape_i_resolves_local_type_aliases() -> None:
    """A local type alias is resolved before the annotation is classified.

    Codex on PR #1306 (`discussion_r4049621555`): `type TransportFailure =
    ClientError` used as `err: TransportFailure` was not seen, because the
    alias name is neither imported nor suffixed. A hundred and eleven forms, each one a
    line of the form table this change was planned and reviewed against; every
    one of them has a representative below (F-68, F-90 and F-91 in the test
    body, because each needs a line thousands of characters long).

    Reported, one marker `# I:` per line in the fixture: the plain statement
    (F-01), a union (F-02), a container (F-03), `Optional` (F-04), a chain of
    hops (F-05), an alias defined *after* its use (F-06), a recursive alias
    with a base case (F-07, its other members carry it), a generic alias whose
    value carries the exception (F-08) and one whose argument does (F-09), an
    alias inside a string annotation (F-10) and one whose value is a string
    (F-11), the conventional form (F-12) also through an import alias of
    `TypeAlias` (F-13), a plain assignment (F-14, the form all six of this
    package's exception aliases use), an alias as the *head* of a subscript
    (F-15) and nested inside one (F-16), an alias on `Annotated` (F-17), on a
    module-qualified type (F-18) and on an import alias (F-19), a variadic
    parameter (F-20), a `match` class pattern (F-21), an alias defined in a
    function (F-22) or in a class body (F-23, scope is flattened on purpose)
    or twice (F-24, every value counts, not the last), an alias as the handler
    type (F-25, reported before this change too), a type parameter bound on a
    function (F-35, both `def` and `async def`), a multiple target (F-37), a
    tuple target (F-38), a bound (F-39) and a PEP 696 default (F-40) at the
    alias head, a qualified access to a class alias (F-41), an alias on
    `Annotated` used as the resolved head (F-48), a doubling union chain
    (F-45, 20 levels, cut by the width limit and reported fail-closed) and a
    linear chain of 70 hops (F-50, cut by the depth limit, which is 64) and
    one on a harmless target (F-51, reported only because the depth limit
    closes fail-closed), a name defined twice with the harmless value last
    (F-53), a
    head chain of two hops (F-54), a head repeated inside its own expansion
    (F-55, a repetition is not a cycle), a string value of a plain assignment
    (F-57), a starred (F-58) and a nested (F-59) tuple target, and a doubling
    chain on a *harmless* leaf (F-60, reported only because the width limit
    closes fail-closed; without the limit it would go silent) and the bound
    of an enclosing function's type parameter in a nested function (F-62), and
    an alias parameter's bound substituted into a string slot (F-64) or into a
    string value (F-65). From the second Codex round on (#1316): the argument
    of a generic alias bound into its value, in a union (F-69), as the whole
    value (F-70), in a string value (F-72), as a string (F-73), through two
    generic aliases (F-86), as a type variable tuple (F-87) and as a surplus
    argument (F-92, fail-closed); a default that names an earlier parameter,
    without (F-74) and with (F-75) an argument; a constraint tuple as the
    value (F-77), on a function (F-78) and in a mapping (F-79); a bounded
    parameter as the argument (F-81); a body that rebinds its own parameter
    (F-83); a class's bounded parameter (F-84); a starred (F-88) and an
    `Unpack`ed (F-89) slot; a bound (F-93) and a default (F-94) beside an
    argument; bound and default at once (F-95); a module alias inside a
    function with a type parameter (F-96); an argument in the first slot of
    `Annotated` (F-98); a union of 1000 members, cut fail-closed (F-91); and
    an argument that holds a generic alias, the same one (F-103, F-104, F-105),
    through a plain alias (F-106) or as a surplus argument (F-107), which is
    no cycle; and a doubling chain of defaults on an unreadable leaf (F-109,
    cut fail-closed: the placeholder of each bound parameter counts toward
    the width limit, without it the expansion runs for half a minute and
    ends silent).

    Reported *and unchanged*, which is the point of the additive expansion:
    an alias with a suffixed name on a harmless value (F-26), one that shadows
    a built-in (F-27) and one whose name shadows a carrier (F-49,
    `Counter = MyCounter` with `err: Counter[ClientError]`). A replacing
    expansion would make all three silent, so the guard would report less
    after learning to resolve more.

    Silent on purpose, marker `# S`, each one with a companion parameter
    (`# C:`) that *is* reported, so the function stands in the pin and only
    the alias member is missing: an own type (F-28), `type[X]` (F-29) and
    `Callable[..., X]` (F-30), which carry a class and not an instance, a
    `TypeVar` (F-31, its value is a call), a cycle without a base case (F-32,
    it names no class an instance can have and it terminates), a name bound in
    another module (F-33), the result of a call (F-34), an unparsable string
    value (F-36, robustness: the alias name stays a member and nothing
    raises), the annotation of a *local* assignment (F-42: reading it as a
    binding would report three sites in this package today), `type` (F-43) and
    `Callable` (F-44) as a resolved head, a value binding (F-46) and a
    resolved value without any type reference (F-47, the counter-test to
    F-14) and an unbounded type parameter whose name another function bounds
    (F-56: the bound of `def f[T: ClientError]` above stays in `f`), and an
    alias parameter's bound that the alias value does not use (F-61) or uses
    inside `Callable` (F-63, as silent as written out), an inherited bound that
    a nested function (F-66) or class (F-67) shadows with its own parameter,
    and `Annotated` whose second argument names the exception (F-52). From
    the second Codex round on: a generic argument inside `Callable` (F-71), a
    generic alias without argument, bound or default (F-76), an alias's own
    parameter against the caller's of the same name (F-80), a function's
    (F-82) and a class's (F-85) parameter that shadows a module alias of the
    same name, a parameter specification (F-97), an argument in the metadata
    of `Annotated` (F-99), a type variable tuple that takes the middle (F-100),
    a sole parameter specification that takes every argument (F-101), a
    `TypeVar` used in a function without type parameters while another
    function bounds the same name (F-102) and a union of 250 members around a
    bounded parameter (F-90), and a bound parameter whose name ends like an
    exception (F-108, the name is no member), and a default (F-110) or a bound
    (F-111) that names its own alias, read with the value's hops. Without
    the companion, a silent case and a misspelled alias name
    would be equally absent from the pin.
    """
    tree = ast.parse(_SHAPE_I_ALIAS_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/alias_fixture.py")
    by_line = sorted((line, leaf) for _, line, leaf, _ in offenders)
    assert by_line == [
        (19, "err"),
        (26, "err"),
        (33, "err"),
        (40, "err"),
        (48, "err"),
        (52, "err"),
        (59, "err"),
        (66, "err"),
        (73, "err"),
        (80, "err"),
        (87, "err"),
        (94, "err"),
        (101, "err"),
        (108, "err"),
        (115, "err"),
        (122, "err"),
        (129, "err"),
        (136, "err"),
        (143, "err"),
        (150, "errors"),
        (159, "caught"),
        (167, "err"),
        (175, "err"),
        (185, "err"),
        (195, "err"),
        (202, "err"),
        (209, "err"),
        (217, "companion"),
        (225, "companion"),
        (233, "companion"),
        (242, "companion"),
        (251, "companion"),
        (256, "companion"),
        (264, "companion"),
        (268, "err"),
        (272, "err"),
        (280, "companion"),
        (287, "err"),
        (294, "err"),
        (301, "err"),
        (308, "err"),
        (312, "err"),
        (321, "companion"),
        (329, "companion"),
        (339, "companion"),
        (366, "err"),
        (375, "companion"),
        (383, "companion"),
        (390, "err"),
        (401, "err"),
        (478, "err"),
        (558, "err"),
        (566, "companion"),
        (576, "err"),
        (584, "err"),
        (591, "err"),
        (596, "companion"),
        (603, "err"),
        (610, "err"),
        (617, "err"),
        (634, "err"),
        (642, "companion"),
        (647, "err"),
        (657, "companion"),
        (664, "err"),
        (671, "err"),
        (677, "companion"),
        (686, "companion"),
        (695, "err"),
        (702, "err"),
        (710, "companion"),
        (717, "err"),
        (721, "err"),
        (728, "err"),
        (735, "err"),
        (740, "companion"),
        (747, "err"),
        (751, "err"),
        (758, "err"),
        (767, "companion"),
        (773, "err"),
        (782, "companion"),
        (791, "err"),
        (798, "err"),
        (804, "companion"),
        (811, "err"),
        (818, "err"),
        (822, "err"),
        (827, "err"),
        (831, "err"),
        (838, "err"),
        (845, "err"),
        (849, "err"),
        (853, "err"),
        (861, "companion"),
        (868, "err"),
        (876, "companion"),
        (886, "companion"),
        (894, "companion"),
        (906, "companion"),
        (913, "err"),
        (920, "err"),
        (924, "err"),
        (931, "err"),
        (935, "err"),
        (943, "companion"),
        (950, "err"),
        (958, "companion"),
        (966, "companion"),
    ]
    outside, _ = _scan_tree(tree, "coordinator/alias_fixture.py")
    assert outside == []
    # F-68: a string whose parse overflows the parser stack (`MemoryError`,
    # measured at 6000 minus signs) is no crash and no finding. It stands here
    # and not in the fixture, which would carry a 6000-character line.
    deep = ast.parse('Deep = "' + "-" * 6000 + '1"')
    assert _scan_tree(deep, "Auth/alias_deep_string.py")[0] == []
    # F-90 and F-91 stand here for the same reason. F-90: a union of 250
    # members around a bounded parameter is no false report (the parameter is
    # bound where the value is walked, not substituted into a copy whose
    # recursion ran out). F-91: a union of 1000 plain members is no crash and
    # is cut fail-closed at the width limit; `ast.unparse` in shape (G) and the
    # union branch here both recursed once per member.
    head = "import logging\n_LOGGER = logging.getLogger(__name__)\n"
    wide = ast.parse(
        head
        + "type Box[T: int] = "
        + " | ".join(["int"] * 250)
        + " | T\n"
        + "def f(err: Box, companion: ClientError) -> None:\n"
        + "    _LOGGER.error('%s %s', err, companion)\n"
    )
    wide_offenders, _ = _scan_tree(wide, "Auth/alias_wide_union.py")
    assert [leaf for _, _, leaf, _ in wide_offenders] == ["companion"]
    widest = ast.parse(
        head
        + "def f(err: "
        + " | ".join(["int"] * 1000)
        + ") -> None:\n"
        + "    _LOGGER.error('%s', err)\n"
    )
    widest_offenders, _ = _scan_tree(widest, "Auth/alias_widest_union.py")
    assert [leaf for _, _, leaf, _ in widest_offenders] == ["err"]
    # The fallback in `_message_params` (shape (G)) reads the names off the
    # nodes: not only no crash, but the message type found at the far end of
    # the same 1000-member union.
    widest_message = ast.parse(
        "def f(msg: " + " | ".join(["int"] * 999) + " | proto.Msg) -> None:\n    pass\n"
    )
    assert _message_params(widest_message.body[0], frozenset({"Msg"})) == {"msg"}
    # V7: the contract sentence has a mechanical reader. Auth/AGENTS.md is the
    # place the review bot quotes from (the finding behind this change carries
    # the anchor `Auth/AGENTS.md:L190-194`); a sentence answering that question
    # without a reader would be a rule nobody enforces.
    contract = (_PACKAGE_ROOT / "Auth" / "AGENTS.md").read_text(encoding="utf-8")
    assert "through a local type alias defined in the same module" in contract


_SHAPE_I_FLOW_FIXTURE = """
import logging
from aiohttp import ClientError
from cryptography import exceptions as cex

_LOGGER = logging.getLogger(__name__)


def _alias(err: ClientError) -> None:
    detail = err
    shown = str(err)
    _LOGGER.error("alias: %s %s", detail, shown)


def _attribute(self, err: ClientError) -> None:
    self.last = err
    _LOGGER.error("attr: %s", self.last)


def _positional(task) -> None:
    err, count = task.exception(), 0
    _LOGGER.error("tuple: %s %s", err, count)


def _starred(task) -> None:
    [first, *rest] = [task.exception(), 0, 1]
    *head, last = 0, 1, task.exception()
    lead, *mid, tail = 0, task.exception(), 1
    _LOGGER.error("star: %s %s %s %s %s %s %s", first, rest, head, last, lead, mid, tail)


def _helper_result(task) -> None:
    a, b = _pick(task)
    _LOGGER.error("helper: %s %s", a, b)


def _loop(errors: list[ClientError]) -> None:
    for err in errors:
        _LOGGER.error("loop: %s", err)
    _LOGGER.error("comp: %s", ", ".join(str(e) for e in errors))


def _match(value, err: ClientError) -> None:
    match value:
        case ClientError() as caught:
            _LOGGER.error("class: %s", caught)
        case str() as text:
            _LOGGER.error("text: %s", text)
    match err:
        case other:
            _LOGGER.error("subject: %s", other)


def _code(err: ClientError) -> None:
    code = err.errno
    kind = _classify(err)
    _LOGGER.error("code: %s %s", code, kind)


def _shadow(err: ClientError) -> None:
    err = "safe"
    _LOGGER.error("shadow: %s", err)


def _from_module(err: cex.InvalidTag) -> None:
    _LOGGER.error("module by from: %s", err)


def _nested_first(err: ClientError) -> None:
    if err:
        detail = err
    shown = str(detail)
    _LOGGER.error("fixpoint: %s", shown)


def _derived(err: ClientError, errors: list[ClientError]) -> None:
    msg = "prefix: "
    msg += str(err)
    joined = "x " + str(err)
    formatted = "x %s" % err
    chosen = err if err else "none"
    first = errors[0]
    pair = (err, 1)
    texts = [str(e) for e in errors]
    glued = ", ".join(str(e) for e in errors)
    reason = err.strerror
    _LOGGER.error("derived: %s %s %s %s %s %s %s %s %s", msg, joined, formatted, chosen, first, pair, texts, glued, reason)


async def _iterables(self, errors: dict[str, ClientError]) -> None:
    for key, err in errors.items():
        _LOGGER.error("items: %s %s", key, err)
    for i, err in enumerate(list(errors.values())):
        _LOGGER.error("enumerate: %s %s", i, err)
    self.errors = errors
    for e in self.errors:
        _LOGGER.error("attribute container: %s", e)
    async for item in _stream(errors):
        _LOGGER.error("async: %s", item)


def _captures(self, value, err: ClientError) -> None:
    match value:
        case ClientError(args=[first]):
            _LOGGER.error("sub-capture: %s", first)
    match str(err):
        case text:
            _LOGGER.error("subject text: %s", text)
    self.errs["k"] = err
    _LOGGER.error("subscript target: %s", self.errs["k"])


def _not_followed(err: ClientError, task) -> None:
    last: str | ClientError | None = _pick(task)
    render = lambda: str(err)
    _LOGGER.error("opaque: %s %s", last, render())


def _more_derived(err: ClientError, errors: list[ClientError]) -> None:
    chosen = getattr(err, "msg", None) or str(err) or ""
    lowered = str(err).lower()
    popped = errors.pop()
    parts = []
    parts.append(str(err))
    code, text = err.args
    head, *tail = errors
    copied = list(errors)
    mapped = ", ".join(map(str, errors))
    _LOGGER.error("more: %s %s %s %s %s %s %s %s %s", chosen, lowered, popped, parts, code, text, head, tail, copied)
    named = getattr(err, "msg", None)
    _LOGGER.error("mapped: %s %s", mapped, named)
    words = describe_exception(err).split()
    bits = err.errno.bit_length()
    count = len(errors)
    kind = getattr(err, "errno", None)
    _LOGGER.error("harmless: %s %s %s %s", words, bits, count, kind)


def _in_handler(task) -> None:
    try:
        pass
    except OSError as exc:
        text = str(exc)
        code, detail = exc.args
        _LOGGER.error("handler flow: %s %s %s", text, code, detail)
    _LOGGER.error("after handler: %s", text)


def _containers(err: ClientError, obj) -> None:
    by_key = {}
    by_key["k"] = err
    grouped = {}
    grouped.setdefault("k", []).append(err)
    collected = []
    [collected.append(str(e)) for e in [err]]
    fallback = getattr(obj, "x", err)
    fields = vars(err)
    _LOGGER.error("containers: %s %s %s %s %s", by_key, grouped, collected, fallback, fields)


def _element_writes(err: ClientError, d, e, f, g) -> None:
    d["k"] += str(err)
    e["a"], e["b"] = err, 1
    f.__setitem__(0, err)
    defaulted = g.get("x", err)
    popped = g.pop("y", err)
    _LOGGER.error("element writes: %s %s %s %s %s", d, e, f, defaulted, popped)


def _exc_info() -> None:
    try:
        pass
    except Exception:
        current = sys.exc_info()[1]
        _LOGGER.error("exc_info: %s %s", sys.exc_info(), current)


def _stdlib_containers(err: ClientError, keys, m, parts) -> None:
    keyed = dict.fromkeys(keys, err)
    queued = deque([err])
    by_keyword = m.get("k", default=err)
    parts.__iadd__([err])
    _LOGGER.error("stdlib: %s %s %s %s", keyed, queued, by_keyword, parts)


def _queue_writes(err: ClientError, q, dq) -> None:
    q.put_nowait(err)
    q.put(str(err))
    dq.extendleft([err])
    _LOGGER.error("queues: %s %s", q, dq)
"""


def test_shape_i_follows_bound_exceptions_by_data_flow() -> None:
    """A name derived from a bound exception inside the function is bound too.

    Reported: an alias and the text of a bound parameter (line 12, `detail`
    and `shown`), an attribute target read back by its dotted name (line 17),
    the positional half of a tuple assignment (line 22, `err` and not
    `count`), starred unpacking matched from both ends with the star
    absorbing the surplus (line 29: `first`, `last` and `mid`, whose
    remainder holds the call; `rest` and `head` collect plain values and
    are not reported; three values against two targets, so a version that
    pairs by index alone binds nothing here), a `for` target and a
    comprehension target over a bound container (lines 39 and 40; line 40
    carries `errors` itself as well), a `match` capture with a foreign
    class pattern (line 46, `caught`; the `str()` capture on line 48 is
    not) and any capture when the subject is bound (line 51), a rebinding
    to a harmless value (line 62: fail-closed, stated in the module
    docstring), a type reached through a module bound by `from` (line 66,
    `cex.InvalidTag`; the Codex finding of 2026-09-18 on `_module_bindings`
    reading `ast.Import` only), a chain whose first link sits in a nested
    block that `ast.walk` visits after the second (line 73, `shown` from
    `detail` from `err`: the fixpoint, not one pass, binds it), nine
    derivations of the text or a container (line 87: `+=`, concatenation,
    `%` formatting, a conditional, an element, a tuple display, a
    comprehension, `join`, a non-code attribute; the review of 2026-09-18
    found each unfollowed), `for` targets whose iterable mentions a bound
    name through `.items()`, `enumerate(list(...))`, an attribute holding
    the container or an `async for` (lines 92, 94, 97, 99; `key` and `i`
    are reported too, fail-closed, the iterable is not typed), a capture
    nested in a foreign class pattern and a subject that is the text (lines
    105 and 108), a subscript target read back by its text and the
    container it writes into (line 110, `self.errs['k']` and `self.errs`),
    nine more derivations (line 129: `or` over `getattr(err, "msg")` and
    `str(err)`, the shape of `auth_flow._describe_lost_session`; a method on
    the text; `errors.pop()`; a list mutated by `append`; both names of an
    unpacking from `err.args` and from the container itself; `list(errors)`)
    with `map(str, errors)` under `join` and `getattr(err, "msg")` on its
    own (line 131; the second review of 2026-09-18 found each unfollowed),
    the text and the unpacked `args` of a handler name inside its handler
    (line 145, the third review's pre-existing gap: the handler seeds the
    same flow) and five container writes (line 158: an element assignment
    binds the mapping, `setdefault(...).append` binds the mapping behind
    the chain, `append` inside a comprehension binds the list, the default
    slot of `getattr`, `vars(err)`), five element writes (line 167: `+=`
    on an element, elements of a tuple target, `__setitem__`, the default
    slot of `.get` and `.pop` on an unbound mapping; the fourth review of
    2026-09-18), `sys.exc_info()` passed directly or through its second slot
    (line 175), and four more container forms (line 183: `dict.fromkeys`,
    `deque`, a keyword default, `__iadd__`; the fifth review) and two queue
    writes (line 190: `put_nowait` and `put` fill an `asyncio.Queue`,
    `extendleft` fills a `deque`, and `str(queue)` renders what is inside
    it; the Codex review of 2026-09-20 on `6ae6af90`, which found the queue
    added as a rendering carrier while its insertion methods were missing
    from the mutator set). Not reported: a tuple target fed by the result
    of a function this file does not define (line 34, `_pick`), a code
    attribute and a classifier's result (line 57, `code` and `kind`), a
    local annotation whose value is such a result and a lambda that is
    called (line 116:
    the package narrows such a name before logging, and a lambda's result
    is a call's), and four harmless derivations (line 136: a method on a
    safe wrapper's result, a method on a code attribute, `len()`, and
    `getattr` naming a code attribute), and the handler alias read after
    the handler (line 146: the flow of a handler name ends with its body;
    `fcmregister.py` narrows such a name before logging it).
    """
    tree = ast.parse(_SHAPE_I_FLOW_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    by_line = sorted((line, leaf) for _, line, leaf, _ in offenders)
    assert by_line == [
        (12, "detail"),
        (12, "shown"),
        (17, "self.last"),
        (22, "err"),
        (29, "first"),
        (29, "last"),
        (29, "mid"),
        (39, "err"),
        (40, "e"),
        (40, "errors"),
        (46, "caught"),
        (51, "other"),
        (62, "err"),
        (66, "err"),
        (73, "shown"),
        (87, "chosen"),
        (87, "first"),
        (87, "formatted"),
        (87, "glued"),
        (87, "joined"),
        (87, "msg"),
        (87, "pair"),
        (87, "reason"),
        (87, "texts"),
        (92, "err"),
        (92, "key"),
        (94, "err"),
        (94, "i"),
        (97, "e"),
        (99, "item"),
        (105, "first"),
        (108, "text"),
        (110, "self.errs"),
        (110, "self.errs['k']"),
        (129, "chosen"),
        (129, "code"),
        (129, "copied"),
        (129, "head"),
        (129, "lowered"),
        (129, "parts"),
        (129, "popped"),
        (129, "tail"),
        (129, "text"),
        (131, "mapped"),
        (131, "named"),
        (145, "code"),
        (145, "detail"),
        (145, "text"),
        (158, "by_key"),
        (158, "collected"),
        (158, "fallback"),
        (158, "fields"),
        (158, "grouped"),
        (167, "d"),
        (167, "defaulted"),
        (167, "e"),
        (167, "f"),
        (167, "popped"),
        (175, "current"),
        (175, "exc_info()"),
        (183, "by_keyword"),
        (183, "keyed"),
        (183, "parts"),
        (183, "queued"),
        (190, "dq"),
        (190, "q"),
    ]
    outside, _ = _scan_tree(tree, "coordinator/fixture.py")
    assert outside == []


_RENDERING_HEADS = (
    "AbstractSet",
    "ChainMap",
    "Collection",
    "Container",
    "Counter",
    "DefaultDict",
    "Deque",
    "Dict",
    "FrozenSet",
    "Future",
    "ItemsView",
    "Iterable",
    "Iterator",
    "KeysView",
    "LifoQueue",
    "List",
    "Mapping",
    "MappingProxyType",
    "MutableMapping",
    "MutableSequence",
    "MutableSet",
    "Optional",
    "OrderedDict",
    "PriorityQueue",
    "Queue",
    "Reversible",
    "Sequence",
    "Set",
    "Task",
    "Tuple",
    "Union",
    "ValuesView",
    "defaultdict",
    "deque",
    "dict",
    "frozenset",
    "list",
    "set",
    "tuple",
)


def test_member_carriers_expose_their_members() -> None:
    """Every rendering head flattens to its members; a dropped entry is caught here.

    The parameter fixture pins three heads; the other entries would vanish
    silently on a later clean-up (review of 2026-09-18, X-03). The list is
    spelled out on purpose: a test that iterated over `_MEMBER_CARRIERS`
    would lose the case together with the entry. One case per head, the
    member is `OSError` in every slot the head takes.
    """
    assert set(_RENDERING_HEADS) == _MEMBER_CARRIERS
    for head in _RENDERING_HEADS:
        annotation = ast.parse(f"{head}[OSError]", mode="eval").body
        members = [ast.unparse(m) for m in _annotation_members(annotation)]
        assert members == ["OSError"], head
    kept = ast.parse("ExceptionGroup[OSError]", mode="eval").body
    assert [ast.unparse(m) for m in _annotation_members(kept)] == ["ExceptionGroup"]


def test_import_helpers_survive_a_hostile_module_getattr(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A module whose `__getattr__` raises contributes nothing and breaks no scan.

    `getattr(module, name, None)` swallows `AttributeError` only; a module
    `__getattr__` (PEP 562) may raise anything, and both import helpers read
    attributes of modules they did not write. The read sits inside the
    `try` (review of 2026-09-18, F-15), so the naming convention decides and
    the scan goes on.
    """

    class _Hostile(types.ModuleType):
        def __getattr__(self, name: str) -> object:
            raise RuntimeError(name)

    def fake_import(name: str) -> types.ModuleType:
        if name == "hostile":
            return _Hostile("hostile")
        raise ImportError(name)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    tree = ast.parse("from hostile import sub\nfrom hostile import Boom as Error\n")
    assert _module_bindings(tree) == {}
    assert _imported_exception_types(tree) == frozenset()


_SHAPE_I_FORWARD_FIXTURE = """
import logging
from aiohttp import ClientError
from .helpers import imported_helper

_LOGGER = logging.getLogger(__name__)


def describe(value) -> None:
    _LOGGER.error("x %s", value)


def f01(err: ClientError) -> None:
    describe(err)


def f02(err: ClientError) -> None:
    describe_kw(cause=err)


def describe_kw(cause) -> None:
    _LOGGER.error("x %s", cause)


def f03(err: ClientError) -> None:
    posonly(1, err)


def posonly(n, v, /) -> None:
    _LOGGER.error("x %s %s", n, v)


def f04(err: ClientError) -> None:
    kwonly(1, cause=err)


def kwonly(n, *, cause) -> None:
    _LOGGER.error("x %s %s", n, cause)


def f05(err: ClientError) -> None:
    star_args(1, err)


def star_args(*args) -> None:
    _LOGGER.error("x %s", args)


def f06(err: ClientError) -> None:
    star_kw(cause=err)


def star_kw(**kw) -> None:
    _LOGGER.error("x %s", kw["cause"])


def f07(err: ClientError) -> None:
    def inner(v) -> None:
        _LOGGER.error("x %s", v)

    inner(err)


class Holder:
    def f08(self, err: ClientError) -> None:
        self.held(err)
        self.static_held(err)

    def held(self, value) -> None:
        _LOGGER.error("x %s", value)

    @staticmethod
    def static_held(value) -> None:
        _LOGGER.error("x %s", value)


def f09(err: ClientError) -> None:
    g = lambda v: lambda_sink(v)
    g(err)


class Receiver:
    async def f10(
        self, entry_id: str, err: ClientError, generation: int | None = None
    ) -> None:
        await self.classify(entry_id, err, generation)

    async def classify(self, entry_id, err, generation=None) -> None:
        _LOGGER.warning("classify %s: %s", entry_id, err)

    async def f11(self, entry_id) -> None:
        try:
            await self.connect()
        except Exception as err:
            await self.classify_handler(entry_id, err)

    async def classify_handler(self, entry_id, problem) -> None:
        _LOGGER.warning("x %s %s", entry_id, problem)


def f12(err: ClientError) -> None:
    text_helper(str(err))


def text_helper(text) -> None:
    _LOGGER.error("x %s", text)


def f13(err: ClientError) -> None:
    safe_helper(describe_exception(err))


def safe_helper(text) -> None:
    _LOGGER.error("x %s", text)


def f14(errors: list[ClientError]) -> None:
    many(*errors)


def many(first, *rest) -> None:
    _LOGGER.error("x %s", first)


def f15(err: ClientError) -> None:
    dict_kw(**{"cause": err})


def dict_kw(cause=None) -> None:
    _LOGGER.error("x %s", cause)


def f16(err: ClientError) -> None:
    hop1(err)


def hop1(a) -> None:
    hop2(a)


def hop2(b) -> None:
    _LOGGER.error("x %s", b)


def f17(err: ClientError) -> None:
    recurse(err, 3)
    ping(err)


def recurse(v, depth=0) -> None:
    if depth:
        recurse(v, depth - 1)
    _LOGGER.error("x %s", v)


def ping(v) -> None:
    pong(v)


def pong(w) -> None:
    ping(w)
    _LOGGER.error("x %s", w)


def f18(err: ClientError) -> None:
    with_default(1, cause=err)


def with_default(x, cause=None) -> None:
    _LOGGER.error("x %s %s", x, cause)


def f19(err: ClientError) -> None:
    quiet(err)


def quiet(v) -> int:
    return len(str(v))


def f20(err: ClientError) -> None:
    later(err)


class First:
    def f21(self, err: ClientError) -> None:
        self.same(err)

    def same(self, v) -> None:
        _LOGGER.error("x %s", v)


class Second:
    def same(self, v) -> None:
        _LOGGER.error("x %s", v)


class Client:
    def f22(self, err: ClientError) -> None:
        self._client.remote(err)

    def remote(self, v) -> None:
        _LOGGER.error("x %s", v)


def f23(err: ClientError) -> None:
    imported_helper(err)


def f24(err: ClientError) -> None:
    as_object(err)
    as_text(str(err))


def as_object(v: object) -> None:
    _LOGGER.error("x %s", v)


def as_text(v: str) -> None:
    _LOGGER.error("x %s", v)


def later(v) -> None:
    _LOGGER.error("x %s", v)


def lambda_sink(v) -> None:
    _LOGGER.error("x %s", v)


def f13b(err: ClientError) -> None:
    kind_helper(_classify(err))


def kind_helper(kind) -> None:
    _LOGGER.error("x %s", kind)


def spread_sink(a, b, c) -> None:
    _LOGGER.error("x %s", c)


def f14(err: ClientError) -> None:
    spread_sink(1, *[err])


class Box:
    def method_sink(self, value) -> None:
        _LOGGER.error("x %s", value)


def f15(err: ClientError) -> None:
    method_sink(1, err)


class Slash:
    def slash_sink(self, /, v) -> None:
        _LOGGER.error("x %s", v)

    def f16(self, err: ClientError) -> None:
        self.slash_sink(v=err)


def posonly_kw_sink(a, /, **kw) -> None:
    _LOGGER.error("x %s", kw)


def f17(err: ClientError) -> None:
    posonly_kw_sink(1, a=err)


def empty_star_sink(a, b=None) -> None:
    _LOGGER.error("x %s", a)


def f18(err: ClientError) -> None:
    empty_star_sink(*(), err)
"""


def test_shape_i_follows_exceptions_into_module_local_helpers() -> None:
    """A bound exception passed to a helper of the same module binds its parameter.

    `_classify_registration_exception(self, entry_id, err, generation)` in
    `fcm_receiver_ha.py` logged a producer's text it received from its
    caller; with the annotation removed, only the caller knows (line 89, the
    historical shape before `97015e1f`). Reported, each in its own helper:
    a positional argument (line 10, the helper defined before its caller)
    and the same helper defined after it (line 224), a keyword argument
    (line 22), a positional-only (line 30) and a keyword-only parameter
    (line 38, not the unrelated `n`), `*args` (line 46) and `**kwargs`
    (line 54), a nested function (line 59), a method called through `self`,
    with the receiver skipped (line 70), and a static method, without (line
    74), a handler name passed on (line 98), the text of the exception (line
    106), a starred list (line 122), a dict unpacked into keywords (line
    130), two hops (line 142), self-recursion and a cycle, both terminating
    (lines 153 and 162), a default parameter given by keyword (line 170),
    a method name shared by two classes, both reported fail-closed (lines
    190 and 195), and parameters annotated `object` or `str` (lines 216 and
    220: the caller decides what they hold, decision V-4). Not reported: a
    helper reached through a lambda (line 79 calls `lambda_sink`, line 228
    stays silent; a lambda that holds the log call itself is a log alias and
    reported at its call, shape (D)), a safe wrapper (line 114), a helper that does not log
    (`quiet`), another receiver (line 203, `self._client.remote` is not
    `Client.remote`), a helper given another function's result (line 236,
    `_classify(err)`: a result is not knowable from the name) and an imported
    helper (`imported_helper`, this guard
    reads one file). A starred argument in the middle of a call binds its
    slot and every positional slot after it (line 240), and a bare name never
    reaches a method of the same name (line 249 stays silent). A receiver
    declared positional-only (`def slash_sink(self, /, v)`) leaves `v`
    reachable by keyword (line 258), and a keyword named like a
    positional-only parameter lands in `**kw` (line 265), and an argument
    after an empty starred one may land in the starred slot (line 273,
    `empty_star_sink(*(), err)`). The same
    fixture outside `Auth/` reports nothing.
    """
    tree = ast.parse(_SHAPE_I_FORWARD_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    by_line = sorted((line, leaf) for _, line, leaf, _ in offenders)
    assert by_line == [
        (10, "value"),
        (22, "cause"),
        (30, "v"),
        (38, "cause"),
        (46, "args"),
        (54, "kw"),
        (59, "v"),
        (70, "value"),
        (74, "value"),
        (89, "err"),
        (98, "problem"),
        (106, "text"),
        (122, "first"),
        (130, "cause"),
        (142, "b"),
        (153, "v"),
        (162, "w"),
        (170, "cause"),
        (190, "v"),
        (195, "v"),
        (216, "v"),
        (220, "v"),
        (224, "v"),
        (240, "c"),
        (258, "v"),
        (265, "kw"),
        (273, "a"),
    ]
    outside, _ = _scan_tree(tree, "coordinator/fixture.py")
    assert outside == []


_SHAPE_I_FORWARD_LIMIT_FIXTURE = """
from aiohttp import ClientError


def start(err: ClientError) -> None:
    one(err)


def one(a) -> None:
    two(a)


def two(b) -> None:
    three(b)


def three(c) -> None:
    _LOGGER.error("x %s", c)
"""


def test_shape_i_forwarding_stops_fail_closed_at_the_round_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Past the round limit every parameter of every module-local callee is bound.

    Three hops need three rounds; with the limit at two the last helper is
    not reached by the fixpoint, and the fail-closed branch still reports it.
    """
    monkeypatch.setitem(globals(), "_FORWARD_ROUND_LIMIT", 2)
    tree = ast.parse(_SHAPE_I_FORWARD_LIMIT_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    assert sorted((line, leaf) for _, line, leaf, _ in offenders) == [(18, "c")]


_SHAPE_I_UNMATCHED_KEYWORD_FIXTURE = """
import logging
from aiohttp import ClientError

_LOGGER = logging.getLogger(__name__)


def keyword_sink(value) -> None:
    _LOGGER.error("x %s", value)


def unmatched(err: ClientError) -> None:
    keyword_sink(1, extra=err)


def matched(err: ClientError) -> None:
    keyword_sink(value=err)
"""


def test_shape_i_unmatched_keyword_without_var_keyword_binds_nothing() -> None:
    """A keyword no parameter takes binds nothing in a helper without `**kwargs`.

    `keyword_sink(1, extra=err)` passes the exception under a name that
    `keyword_sink(value)` does not declare, and the helper has no `**kwargs`
    to collect it; the call cannot bind `value` to the exception, so line 9
    is not reported for it. The same helper given the exception as
    `value=err` is reported at line 9, which shows that the fixture reaches
    the sink.
    """
    tree = ast.parse(_SHAPE_I_UNMATCHED_KEYWORD_FIXTURE)
    seeds = _forwarded_parameter_seeds(
        tree,
        _exception_names_by_call(tree, set()),
        _module_local_callees(tree),
    )
    assert sorted(seeds.values()) == [("value",)]
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    assert sorted((line, leaf) for _, line, leaf, _ in offenders) == [(9, "value")]
    without_match = ast.parse(
        _SHAPE_I_UNMATCHED_KEYWORD_FIXTURE.partition("\n\n\ndef matched")[0]
    )
    offenders, _ = _scan_tree(without_match, "Auth/fixture.py")
    assert offenders == []


_SHAPE_I_RESULT_FIXTURE = """
import logging
from aiohttp import ClientError
from .helpers import imported_pick

_LOGGER = logging.getLogger(__name__)


def pick(task):
    return task.exception()


def f01(task) -> None:
    err = pick(task)
    _LOGGER.error("x %s", err)


def ident(value):
    return value


def f02(e: ClientError) -> None:
    err = ident(e)
    _LOGGER.error("x %s", err)


async def apick(task):
    return task.exception()


async def f03(task) -> None:
    err = await apick(task)
    _LOGGER.error("x %s", err)


def f04(task) -> None:
    if (err := pick(task)) is not None:
        _LOGGER.error("x %s", err)


def f05(task) -> None:
    err: BaseException | None = pick(task)
    _LOGGER.error("x %s", err)


class Client:
    def _pick(self, task):
        return task.exception()

    def run(self, task) -> None:
        err = self._pick(task)
        _LOGGER.error("x %s", err)


def catch():
    try:
        work()
    except ClientError as caught:
        return caught
    return None


def f07() -> None:
    err = catch()
    _LOGGER.error("x %s", err)


def outer(task):
    return pick(task)


def f08(task) -> None:
    err = outer(task)
    _LOGGER.error("x %s", err)


def kept(task):
    found = task.exception()
    return found


def f09(task) -> None:
    err = kept(task)
    _LOGGER.error("x %s", err)


def tell(task):
    return "fixed"


def f10(task) -> None:
    text = tell(task)
    _LOGGER.error("x %s", text)


def wrapper(task):
    def inner():
        return task.exception()

    inner()
    return "done"


def f11(task) -> None:
    status = wrapper(task)
    _LOGGER.error("x %s", status)


def f12(err: ClientError) -> None:
    kind = _classify(err)
    _LOGGER.error("x %s", kind)


def f13(task) -> None:
    err = imported_pick(task)
    _LOGGER.error("x %s", err)


def describe_exception(task):
    return task.exception()


def f14(task) -> None:
    kind = describe_exception(task)
    _LOGGER.error("x %s", kind)


def relay(task):
    found = pick(task)
    return found


def f15(task) -> None:
    err = relay(task)
    _LOGGER.error("x %s", err)
"""


def test_shape_i_follows_helper_results() -> None:
    """A module-local helper whose `return` carries a bound exception binds its result.

    `err = pick(task)` hides a producer's text behind a call; this guard reads
    one file, so it follows a result where that file proves it: a helper of
    the same module that returns `<task>.exception()` (line 15), a bound
    parameter (line 24, the parameter bound by forwarding), through `await`
    (line 33), a walrus (line 38), an annotated assignment (line 43), a method
    called through `self` (line 52), a foreign handler name (line 65), the
    result of another such helper (line 74, two hops), a name bound inside
    the helper (line 84) and a name bound inside the helper from such a
    result (line 135, one more round of the fixpoint). Not reported: a helper that returns a constant
    (`tell`), a helper whose only carrying `return` sits in a nested function
    (`wrapper`), a function the module does not define (`_classify`,
    `imported_pick`: their result is not knowable from one file, EB-09 stays
    declared for them) and a local function named like a safe wrapper
    (`describe_exception`). The same fixture outside `Auth/` reports nothing.
    """
    tree = ast.parse(_SHAPE_I_RESULT_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    assert sorted((line, leaf) for _, line, leaf, _ in offenders) == [
        (15, "err"),
        (24, "err"),
        (33, "err"),
        (38, "err"),
        (43, "err"),
        (52, "err"),
        (65, "err"),
        (74, "err"),
        (84, "err"),
        (135, "err"),
    ]
    outside, _ = _scan_tree(tree, "coordinator/fixture.py")
    assert outside == []


_SHAPE_I_RESULT_LIMIT_FIXTURE = """
def first(task):
    return task.exception()


def second(task):
    return first(task)


def start(task) -> None:
    err = second(task)
    _LOGGER.error("x %s", err)
    _LOGGER.error("x %s", second(task))


def describe_exception(task):
    return task.exception()


def quiet(task) -> None:
    kind = describe_exception(task)
    _LOGGER.error("x %s", kind)
    _LOGGER.error("x %s", describe_exception(task))
"""


def test_shape_i_helper_results_stop_fail_closed_at_the_round_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Past the round limit every helper that returns a value binds its result.

    `second` returns a result only once `first` is known to, one round later;
    with the limit at one the fixpoint stops before, and the fail-closed
    branch still reports line 12 and the result used in place (line 13). A
    local function named like a safe wrapper (`describe_exception`) stays
    out of the fail-closed branch too.
    """
    monkeypatch.setitem(globals(), "_FORWARD_ROUND_LIMIT", 1)
    tree = ast.parse(_SHAPE_I_RESULT_LIMIT_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    assert sorted((line, leaf) for _, line, leaf, _ in offenders) == [
        (12, "err"),
        (13, "second()"),
    ]


_SHAPE_I_RESULT_INPLACE_FIXTURE = """
import logging
from aiohttp import ClientError
from .helpers import imported_pick

_LOGGER = logging.getLogger(__name__)


class OwnError(Exception):
    pass


def pick(task):
    return task.exception()


async def apick(task):
    return task.exception()


def tell(task):
    return "fixed"


def f01(task) -> None:
    _LOGGER.error("x %s", pick(task))


def f02(task) -> None:
    _LOGGER.error("x %s", str(pick(task)))


def f03(task) -> None:
    _LOGGER.error(f"x {pick(task)}")


def f04(task) -> None:
    _LOGGER.error("x %s", pick(task).args)


async def f05(task) -> None:
    _LOGGER.error("x %s", await apick(task))


def f06(task) -> None:
    raise OwnError(str(pick(task)))


class Holder:
    def pick(self, task):
        return task.exception()

    def f07(self, task) -> None:
        _LOGGER.error("x %s", self.pick(task))


def f08(task) -> None:
    _LOGGER.error("x %s", pick(task) or None, pick(task)[0])


def f09(task) -> None:
    _LOGGER.error("x %s", pick(task).errno)


def f10(task) -> None:
    _LOGGER.error("x %s", describe_exception(pick(task)))


def f11(task) -> None:
    _LOGGER.error("x %s", tell(task))


def f12(task) -> None:
    _LOGGER.error("x %s", imported_pick(task))


def f13(err: ClientError) -> None:
    _LOGGER.error("x %s", pick(err))
"""


def test_shape_i_reports_helper_results_used_in_place() -> None:
    """A module-local helper whose `return` carries is reported where its result is used.

    `_LOGGER.error("%s", pick(task))` has no name to bind; the call itself is
    the leaf, like `task.exception()` passed directly. Reported: the bare
    result (line 26), `str()` around it (line 30), an f-string (line
    34), `.args` (line 38), behind `await` (line 42), in the constructor of an
    own exception (line 46), a method called through `self` (line 54), and `or`
    or a subscript around it (line 58, one leaf per line). Not reported: a code
    attribute (`.errno`, line 62), the result inside a safe wrapper (line
    66), a helper that returns a constant (line 70), and a function this
    file does not define (line 74). A call whose own argument is a bound name
    reports the name, not the call a second time (line 78). The same fixture
    outside `Auth/` reports nothing.
    """
    tree = ast.parse(_SHAPE_I_RESULT_INPLACE_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    assert sorted((line, leaf) for _, line, leaf, _ in offenders) == [
        (26, "pick()"),
        (30, "pick()"),
        (34, "pick()"),
        (38, "pick()"),
        (42, "apick()"),
        (46, "pick()"),
        (54, "self.pick()"),
        (58, "pick()"),
        (78, "err"),
    ]
    outside, _ = _scan_tree(tree, "coordinator/fixture.py")
    assert outside == []


_SHAPE_I_CONSTRUCTOR_FIXTURE = """
import logging
from aiohttp import ClientError
from .browser_deps import missing_dependency

_LOGGER = logging.getLogger(__name__)


class OwnError(Exception):
    pass


def f01(err: ClientError) -> None:
    raise OwnError(str(err)) from err


def f02(err: ClientError) -> None:
    raise OwnError(f"login failed: {err}")


def f03(err: ClientError) -> None:
    wrapped = OwnError(err)
    raise wrapped


def f04() -> None:
    try:
        run()
    except ImportError as _err:
        raise missing_dependency(_err) from _err


def f05(err: ClientError) -> None:
    raise OwnError(describe_exception(err)) from err


def f06(err: ClientError) -> None:
    message = str(err)
    raise OwnError(f"failed ({len(message)} chars)") from err


def f07(err: ClientError) -> None:
    raise OwnError("login failed") from err


def f08(err: ClientError) -> None:
    raise OwnError(_describe_aas_error_text(str(err))) from err
"""


def test_shape_i_reads_own_exceptions_built_from_foreign_ones() -> None:
    """The arguments of an exception's constructor are read like a log call's.

    An own exception built from a foreign one carries the producer's text
    one hop later (`97015e1f` lists the form next to the alias and
    `traceback.format_exc()`); `auth_flow.py` raises
    `missing_browser_dependency(_err)`, whose message quotes `_err`
    (`browser_deps.py`). Reported, under the format marker `raise`: the
    text of the exception (line 14), an f-string (line 18), the two-step
    form `wrapped = OwnError(err)` (line 22) and a function the module does
    not define, given a handler name (line 30). Not reported: a safe
    wrapper (`describe_exception`), the length of a bound text (`len`), a
    fixed message with the foreign cause chained (`from err` renders the
    cause only where a traceback is logged, which shape (I) reports at the
    log call) and the package's own description of a gpsoauth error text
    (`_describe_aas_error_text`: markers and length). The same fixture
    outside `Auth/` reports nothing.
    """
    tree = ast.parse(_SHAPE_I_CONSTRUCTOR_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    assert sorted((line, leaf, fmt) for _, line, leaf, fmt in offenders) == [
        (14, "err", "raise"),
        (18, "err", "raise"),
        (22, "err", "raise"),
        (30, "_err", "raise"),
    ]
    outside, _ = _scan_tree(tree, "coordinator/fixture.py")
    assert outside == []


_SHAPE_I_MANAGER_FIXTURE = """
import contextlib
import logging
from aiohttp import ClientError

_LOGGER = logging.getLogger(__name__)


def pick(task):
    return task.exception()


@contextlib.contextmanager
def caught(task):
    yield task.exception()


@contextlib.contextmanager
def relay(value):
    yield value


@contextlib.asynccontextmanager
async def acaught(task):
    yield task.exception()


@contextlib.contextmanager
def via(task):
    yield pick(task)


@contextlib.contextmanager
def guarded():
    try:
        yield None
    except ClientError as caught_err:
        yield caught_err


@contextlib.contextmanager
def harmless(err: ClientError):
    yield describe_exception(err)


@contextlib.contextmanager
def outer_cm(err: ClientError):
    def inner():
        yield err

    yield "fixed"


def f01(task) -> None:
    with caught(task) as err:
        _LOGGER.error("failed: %s", err)


def f02(err: ClientError) -> None:
    with relay(err) as again:
        _LOGGER.error("failed: %s", again)


async def f03(task) -> None:
    async with acaught(task) as err:
        _LOGGER.error("failed: %s", err)


def f04(task) -> None:
    with via(task) as err:
        _LOGGER.error("failed: %s", err)


def f05() -> None:
    with guarded() as err:
        _LOGGER.error("failed: %s", err)


def f06(err: ClientError) -> None:
    with harmless(err) as kind:
        _LOGGER.error("failed: %s", kind)


def f07(err: ClientError) -> None:
    with outer_cm(err) as text:
        _LOGGER.error("failed: %s", text)


def f08(cm) -> None:
    with cm as err:
        _LOGGER.error("failed: %s", err)


def f09(path) -> None:
    with open(path) as fh, caught(path) as err:
        _LOGGER.error("failed: %s %s", fh, err)


async def fetch(task):
    return task.exception()


@contextlib.asynccontextmanager
async def awaited(task):
    yield await fetch(task)


async def f10(task) -> None:
    async with awaited(task) as err:
        _LOGGER.error("failed: %s", err)
"""


def test_shape_i_follows_exceptions_yielded_by_local_context_managers() -> None:
    """A `with … as` target binds when a module-local manager yields an exception.

    `contextlib.contextmanager` hands what the generator yields to the `as`
    target; when the generator is defined in this file, its `yield` is read
    like a helper's `return` (`_yielding_calls`). Reported: a yielded
    `task.exception()` (line 56), a forwarded parameter yielded back
    (line 61), `async with` (line 66), a yielded helper result (line 71), a
    foreign handler name yielded inside the handler (line 76), the second
    item of a two-item `with` (line 96) and an awaited helper result
    yielded by an async manager (line 110). Not reported: a safe wrapper
    yielded (`describe_exception`, line 81), a `yield` in a nested function
    (line 86), a manager the file does not define (the parameter `cm`,
    line 91) and `open(path)` (line 96, `fh`). The same fixture outside
    `Auth/` reports nothing.
    """
    tree = ast.parse(_SHAPE_I_MANAGER_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    assert sorted((line, leaf) for _, line, leaf, _ in offenders) == [
        (56, "err"),
        (61, "again"),
        (66, "err"),
        (71, "err"),
        (76, "err"),
        (96, "err"),
        (110, "err"),
    ]
    outside, _ = _scan_tree(tree, "coordinator/fixture.py")
    assert outside == []


_SHAPE_I_MANAGER_LIMIT_FIXTURE = """
def first(task):
    return task.exception()


def manager(task):
    yield first(task)


def start(task) -> None:
    with manager(task) as err:
        _LOGGER.error("x %s", err)
"""


def test_shape_i_manager_yields_stop_fail_closed_at_the_round_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Past the round limit every module-local generator that yields binds.

    `manager` yields an exception only once `first` is known to return one,
    one round later; with the limit at one the fixpoint stops before, and
    the fail-closed branch still reports line 12.
    """
    monkeypatch.setitem(globals(), "_FORWARD_ROUND_LIMIT", 1)
    tree = ast.parse(_SHAPE_I_MANAGER_LIMIT_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    assert sorted((line, leaf) for _, line, leaf, _ in offenders) == [(12, "err")]


def test_shape_i_reports_producer_exceptions_in_broad_auth_handlers() -> None:
    """Shape (I): seven sinks carry a foreign exception, four are safe.

    A wrapper clears only its own argument (line 15), a builtin type is
    foreign (line 17), a type defined in the module is not (line 19, but its
    `exc_info` is: an own exception raised `from` a foreign one renders the
    cause). A traceback is a sink wherever it is attached: `exc_info=exc`
    (line 14), `logger.exception` (line 28), `exc_info=True` (29), an alias
    bound in the handler and logged outside it (32), a tuple (33);
    `exc_info=False` (30) and `exc_info=None` (31) are not. The same fixture
    outside `Auth/` reports nothing, so the scope is measured and not assumed.
    """
    tree = ast.parse(_SHAPE_I_FIXTURE)
    offenders, _ = _scan_tree(tree, "Auth/fixture.py")
    by_line = sorted((line, leaf) for _, line, leaf, _ in offenders)
    assert by_line == [
        (6, "exc"),
        (7, "exc"),
        (8, "exc"),
        (9, "exc"),
        (10, "exc"),
        (14, "exc_info"),
        (15, "exc"),
        (17, "builtin_type"),
        (19, "exc_info"),
        (28, "exc_info"),
        (29, "exc_info"),
        (32, "exc_info"),
        (33, "exc_info"),
    ]
    outside, _ = _scan_tree(tree, "coordinator/fixture.py")
    assert outside == []


def test_exc_info_is_skipped_by_shape_i_only() -> None:
    """A payload passed as `exc_info=` is a shape-(D) finding, not only a traceback one."""
    source = """
def read(response):
    body = response.read()
    try:
        return parse(body)
    except Exception as exc:
        _LOGGER.error("failed", exc_info=body)
"""
    offenders, _ = _scan_tree(ast.parse(source), "Auth/fixture.py")
    assert sorted((line, leaf) for _, line, leaf, _ in offenders) == [
        (7, "body"),
        (7, "exc_info"),
    ]


def test_former_pinned_file_is_in_shape_i_scope() -> None:
    """`fcm_receiver_ha.py` is scanned for shape (I) like any other Auth module.

    Three assertions in one test, none of which carries the proof alone: the
    real file has no offender, it is actually scanned (`scanned` counts the
    log calls regardless of scope, so an empty offender list on its own would
    be a vacuum), and the shape-(I) fixture under this very path reports the
    same 13 sites as under `Auth/fixture.py`, which is what proves that shape
    (I) applies on the path. A new pin would need a new follow-up plan; none
    exists.
    """
    relative = "Auth/fcm_receiver_ha.py"
    path = _PACKAGE_ROOT / relative
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    offenders, scanned = _scan_tree(tree, relative)
    assert offenders == []
    assert scanned > 100, scanned
    fixture = ast.parse(_SHAPE_I_FIXTURE)
    under_path, _ = _scan_tree(fixture, relative)
    under_fixture, _ = _scan_tree(fixture, "Auth/fixture.py")
    by_line = sorted((line, leaf) for _, line, leaf, _ in under_path)
    assert by_line == sorted((line, leaf) for _, line, leaf, _ in under_fixture)
    assert len(by_line) == 13
