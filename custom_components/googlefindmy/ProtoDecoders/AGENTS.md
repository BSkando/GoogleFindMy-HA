# ProtoDecoders/AGENTS.md — Protobuf overlay expectations

**Scope:** Applies to all stub overlays under `custom_components/googlefindmy/ProtoDecoders/`.

## Generated message classes must inherit `google.protobuf.message.Message`

When updating or regenerating the protobuf stub overlays in this directory:

* Import `Message` from `google.protobuf.message` and alias it locally when needed (for example, `from google.protobuf import message as _message; Message = _message.Message`).
* Ensure every generated message class directly subclasses this concrete base (`class DeviceUpdate(Message): ...`). This maintains nominal subtyping so helpers typed against `google.protobuf.message.Message` continue to accept the generated stubs.
* Protocol helpers from `custom_components.googlefindmy.protobuf_typing` may still be used alongside the concrete inheritance. Prefer composition (e.g., aliasing `EnumTypeWrapperMeta`) rather than replacing the base class with a protocol.

Breaking this contract causes strict mypy runs to treat generated messages as incompatible with helper signatures expecting `Message`.

## NEVER vendor types from the `google.*` namespace

Types that already exist in the official `protobuf` or `googleapis-common-protos` packages **must not** be re-defined here. Vendoring them causes a **duplicate-symbol crash** on Python >= 3.13 when another Home Assistant integration (e.g. Nest, Google Cloud TTS) loads the official library into the process-wide default descriptor pool.

Concrete rules:

* **`google.protobuf.Any`** — use `google.protobuf.any_pb2` from the `protobuf` package. A vendored `Any_pb2.py` was removed for this reason.
* **`google.rpc.Status`** — a vendored `RpcStatus_pb2.py` is kept as fallback because `googleapis-common-protos` is not a declared dependency. `nova_request.py` prefers the official `google.rpc.status_pb2` when available and falls back to the vendored copy. See the import cascade in `nova_request.py:55-74`.
* **Any new `.proto` with `package google.*`** — do not add one. Import the official module at runtime instead.

### Descriptor pool architecture

Every `_pb2.py` file in this project uses a **separate `DescriptorPool()`** instead of the process-wide default pool. This prevents symbol collisions with types that other integrations may register.

| Module | Pool variable | Shared with |
|--------|---------------|-------------|
| `RpcStatus_pb2.py` | `_rpc_pool` | — (seeds official `any_pb2` descriptor) |
| `Common_pb2.py` | `_common_pool` | `DeviceUpdate_pb2`, `LocationReportsUpload_pb2` |
| `DeviceUpdate_pb2.py` | `_findmy_pool` | shares `_common_pool` |
| `LocationReportsUpload_pb2.py` | `_findmy_pool` | shares `_common_pool` |
| Firebase modules | `_firebase_pool` | shared between `android_checkin_pb2`, `checkin_pb2`, `mcs_pb2` |

When adding a new proto module that depends on an existing one, **reuse the parent's pool** (e.g. `_findmy_pool = Common_pb2._common_pool`) so that cross-file type references resolve.

Regression tests: `tests/test_protobuf_namespace_conflict.py`.

## Regeneration checklist (developer workflow)

Use the checked-in proto sources (`custom_components/googlefindmy/ProtoDecoders/*.proto`) and regenerate overlays from the repository root:

1. Ensure `protoc` >= 24 is installed locally and on the `PATH`.
2. Run `python -m custom_components.googlefindmy.ProtoDecoders.decoder`. The module's `__main__` hook orchestrates the required `protoc` invocations for both `.py` and `.pyi` outputs.
3. **After regeneration**, manually replace the default pool (`_descriptor_pool.Default()`) with a separate pool variable in each new `_pb2.py` file. `protoc` does not generate this — it must be patched by hand.
4. Verify the generated `.pyi` stubs keep `Message = _message.Message` and subclass `Message` directly before committing changes.

If the upstream proto schema changes, update the mirrored definitions under `custom_components/googlefindmy/ProtoDecoders/*.proto` first so regenerations remain reproducible from source control.

## Generator version

`_pb2.py` must keep the header `Protobuf Python Version: 6.31.1`, because `pyproject.toml` requires `protobuf >= 6.31.1` and generated code refuses older runtimes. `grpcio-tools==1.76.0` (measured, also 1.74.0 and 1.75.0) emits that header; install it in a separate venv, not in the test venv. It works without a system `protoc`: `python -m grpc_tools.protoc -I custom_components/googlefindmy --python_out=<dir> --pyi_out=<dir> ProtoDecoders/DeviceUpdate.proto`. Keep the header block of the checked-in `_pb2.py` (pool, import from `custom_components.googlefindmy.ProtoDecoders`, `ruff: noqa`) and replace `_descriptor_pool.Default()` as described above. The checked-in `.pyi` files are hand-shaped: classes are `(Message, _MessageProto)`, enums use `metaclass=EnumTypeWrapper`; do not overwrite them with raw `--pyi_out` output.

## Type rule for the device-list parse path

Every length-delimited field that is newly declared on a message reachable from `DevicesList` or `DeviceUpdate` is `bytes`, without exception, even where the source of the schema (for example Spot) declares a message or a string. A declared `string` with invalid UTF-8 or a declared message with foreign content raises `DecodeError`, and `parse_device_list_protobuf` turns that into a failure of the whole device list. A `bytes` field cannot fail, and a wrong wire type lands in the unknown fields. Keep the intended shape as its own message and name it in the field comment, then read the field through a guarded helper only (`trackable_components_of` in `decoder.py` is the pattern). Varint fields are safe. `tests/test_decoder_spot_schema.py` pins the list of length-delimited fields on this path.

Unknown fields on this path are reported at DEBUG only as the root `AGENTS.md` describes under **Schema-drift diagnostics** (section 5, "Never log"); a known `bytes` field is opened there only through `_FORM_BY_FIELD` in `custom_components/googlefindmy/unknown_field_tree.py`, so a newly declared field that carries key material or identifiers stays closed unless it is added to that list.

Scalar fields holding key material or personal data go into `_SECRET_PROTO_FIELDS` in `decoder.py`; the same test file fails on a new field whose name suggests either and that is not listed or explained. The set matches by field name and only on scalar fields, so a message field of a listed name stays visible and shows its own fields one by one.

## Schema additions from Spot

Messages and fields marked `Spot@55f87f62` come from [leonboe1/Spot](https://github.com/leonboe1/Spot), commit `55f87f62302fae24150188124e8e07d5285568d8` (GPL-3.0). They are reverse-engineered and not confirmed by Google. Messages the integration does not use yet are kept on purpose so the wire format does not have to be guessed later. `docs/FMDN.md` section S8.4 holds the inventory.

## Proto3 scalar presence reminder

Proto3 **non-optional scalar fields do not support `HasField`**. Access them directly and rely on their default values (for example, `0` for integers) instead of calling presence checks that raise `ValueError`.
