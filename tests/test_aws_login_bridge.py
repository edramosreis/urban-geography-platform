"""Temporary-login parsing, private pipes and fixed identity admission without AWS."""

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
for name in ("aws_login_bridge", "aws_login_probe"):
    if name in sys.modules:
        continue
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / (name + ".py"))
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
bridge = sys.modules["aws_login_bridge"]
worker = sys.modules["aws_login_probe"]
OWNER = "123456789012"
# Synthetic invalid-for-AWS material; never contact a service with it.
KEY = "ASIA" + "A" * 16
SECRET = "s" * 40
TOKEN = "dummySessionToken"


@pytest.fixture
def record():
    return {
        "Version": 1,
        "AccessKeyId": KEY,
        "SecretAccessKey": SECRET,
        "SessionToken": TOKEN,
        "Expiration": (datetime.now(UTC) + timedelta(minutes=10)).isoformat(),
        "AccountId": OWNER,
    }


@pytest.fixture
def sdk_root(tmp_path, monkeypatch):
    monkeypatch.setattr(bridge, "ROOT", tmp_path)
    path = tmp_path / "artifacts" / "sdk"
    path.mkdir(parents=True)
    return path


def test_temporary_roundtrip_hides_repr_and_drops_owned_references(record):
    credentials = bridge.temporary(record)
    assert (
        KEY not in repr(credentials)
        and SECRET not in repr(credentials)
        and TOKEN not in repr(credentials)
    )
    restored, owner = bridge.receive(bridge.envelope(credentials, OWNER))
    assert owner == OWNER and restored.session_options() == credentials.session_options()
    credentials.clear()
    assert set(credentials.session_options().values()) == {""}


@pytest.mark.parametrize(
    "change",
    [
        {"Version": True},
        {"Version": 2},
        {"AccessKeyId": "AKIA" + "A" * 16},
        {"SecretAccessKey": "short"},
        {"SessionToken": ""},
        {"SessionToken": "line\nbreak"},
        {"SessionToken": "a" * 16385},
        {"Expiration": "tomorrow"},
        {"Expiration": "2026-11-03T00:00:00"},
        {"Expiration": "2026-11-03T00:00:00+01:00"},
        {"Expiration": "2026-99-03T00:00:00Z"},
        {"AccountId": "other"},
        {"extra": SECRET},
    ],
)
def test_bad_exports_have_only_constant_errors(record, change):
    with pytest.raises(bridge.BridgeError) as error:
        bridge.temporary(record | change)
    assert all(value not in str(error.value) for value in (KEY, SECRET, TOKEN))


@pytest.mark.parametrize("missing", ["Version", "SessionToken", "Expiration"])
def test_nonexpiring_or_incomplete_exports_are_not_accepted(record, missing):
    del record[missing]
    with pytest.raises(bridge.BridgeError):
        bridge.temporary(record)


@pytest.mark.parametrize(
    "seconds,accepted", [(59, False), (60, True), (43200, True), (43201, False), (-1, False)]
)
def test_expiration_window_is_explicit(record, seconds, accepted):
    now = datetime(2026, 11, 3, tzinfo=UTC)
    record["Expiration"] = (now + timedelta(seconds=seconds)).isoformat()
    if accepted:
        bridge.temporary(record, now)
    else:
        with pytest.raises(bridge.BridgeError, match="outside_window"):
            bridge.temporary(record, now)


def test_optional_account_id_is_accepted_but_a_different_account_is_rejected(record):
    no_owner = record.copy()
    del no_owner["AccountId"]
    assert bridge.temporary(no_owner).owner is None
    with pytest.raises(bridge.BridgeError, match="account_differs"):
        bridge.envelope(bridge.temporary(record), "999999999999")
    packet = {"version": 1, "expected_account": "999999999999", "credentials": record}
    with pytest.raises(bridge.BridgeError, match="account_differs"):
        bridge.receive(json.dumps(packet).encode())


@pytest.mark.parametrize(
    "content", [b"", b"x" * (bridge.MAX_BYTES + 1), b"[]", b'{"a":1,"a":2}', b"\xff", b"[" * 2000]
)
def test_untrusted_json_is_bounded_and_duplicate_keys_rejected(content):
    with pytest.raises(bridge.BridgeError):
        bridge.decode(content)


@pytest.mark.parametrize(
    "change",
    [
        {"version": True},
        {"version": 2},
        {"credentials": []},
        {"extra": "x"},
        {"expected_account": "wrong"},
    ],
)
def test_envelope_schema_is_closed(record, change):
    packet = {"version": 1, "expected_account": OWNER, "credentials": record} | change
    with pytest.raises(bridge.BridgeError):
        bridge.receive(json.dumps(packet).encode())


def test_export_uses_one_named_login_profile_and_scrubs_ambient_credentials(record, monkeypatch):
    capture = Mock(
        side_effect=[
            f"arn:aws:iam::{OWNER}:role/managed/ProjectAdmin".encode(),
            json.dumps(record).encode(),
        ]
    )
    monkeypatch.setattr(bridge, "capture", capture)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "unexpected")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://example.invalid")
    monkeypatch.setenv("AWS_CONFIG_FILE", "/other/config")
    monkeypatch.setenv("BOTO_CONFIG", "/other/boto")
    result = bridge.export_login("aws.exe", "urban-geo", OWNER)
    assert result.access_key == KEY and capture.call_count == 2
    first, second = capture.call_args_list
    assert first.args[0][:4] == ["aws.exe", "configure", "get", "login_session"]
    assert second.args[0][:5] == [
        "aws.exe",
        "configure",
        "export-credentials",
        "--format",
        "process",
    ]
    for call in capture.call_args_list:
        assert "urban-geo" in call.args[0] and "eu-north-1" in call.args[0]
        assert all(value not in str(call.args) for value in (KEY, SECRET, TOKEN))
        environment = call.kwargs["environment"]
        assert "AWS_ACCESS_KEY_ID" not in environment and "AWS_ENDPOINT_URL" not in environment
        assert "AWS_CONFIG_FILE" not in environment and "BOTO_CONFIG" not in environment


@pytest.mark.parametrize("session", [b"", b"not-a-login", b"arn:aws:iam::999999999999:role/admin"])
def test_missing_or_wrong_account_login_stops_before_credential_export(
    record, monkeypatch, session
):
    capture = Mock(return_value=session)
    monkeypatch.setattr(bridge, "capture", capture)
    with pytest.raises(bridge.BridgeError):
        bridge.export_login("aws.exe", "urban-geo", OWNER)
    assert capture.call_count == 1


def test_profile_validation_precedes_any_command(monkeypatch):
    capture = Mock()
    monkeypatch.setattr(bridge, "capture", capture)
    with pytest.raises(bridge.BridgeError):
        bridge.export_login("aws.exe", "--debug", OWNER)
    capture.assert_not_called()


def test_real_child_capture_bounds_output_and_suppresses_stderr():
    assert (
        bridge.capture(
            [sys.executable, "-c", "import sys;sys.stderr.write('hidden');print('safe')"]
        )
        == b"safe\n"
    )
    for source, maximum, seconds in [
        ("print('x'*10000)", 100, 3),
        ("import time;time.sleep(5)", 100, 0.1),
        ("import sys;sys.stderr.write('hidden');sys.exit(2)", 100, 3),
    ]:
        with pytest.raises(bridge.BridgeError) as error:
            bridge.capture([sys.executable, "-c", source], maximum=maximum, seconds=seconds)
        assert "hidden" not in str(error.value)


def test_real_private_pipe_is_not_a_command_argument(record):
    content = json.dumps(record).encode()
    result = bridge.capture(
        [sys.executable, "-c", "import sys;raw=sys.stdin.buffer.read();print(len(raw))"],
        content=content,
    )
    assert int(result) == len(content)


@pytest.mark.parametrize("sdk", ["glue", "analytics"])
def test_docker_command_only_mounts_code_and_existing_sdk_without_secret_settings(
    record, sdk_root, sdk
):
    command = bridge.docker_command(
        ["docker"], sdk, sdk_root, "sha256:" + "a" * 64, "urban-geo-login-" + "b" * 16
    )
    assert all(value not in str(command) for value in (KEY, SECRET, TOKEN))
    assert "AWS_ACCESS_KEY_ID" not in str(command) and "AWS_SESSION_TOKEN" not in str(command)
    mounts = [command[index + 1] for index, value in enumerate(command) if value == "--mount"]
    assert len(mounts) == 4 and all(item.endswith(",readonly") for item in mounts)
    assert not any(".aws" in item or "target=/workspace," in item for item in mounts)
    assert "--read-only" in command and command[command.index("--network") + 1] == "bridge"
    assert command[command.index("--memory") + 1] == "4g"
    assert command[command.index("--cpus") + 1] == "2"
    offline = bridge.docker_command(
        ["docker"], sdk, sdk_root, "sha256:" + "a" * 64, "urban-geo-login-" + "b" * 16, offline=True
    )
    assert offline[-1] == "--offline" and offline[offline.index("--network") + 1] == "none"


@pytest.mark.parametrize(
    "sdk,path", [("other", "artifacts/sdk"), ("glue", "data/sdk"), ("glue", "absent")]
)
def test_other_sdk_or_directory_is_not_mounted(tmp_path, monkeypatch, sdk, path):
    monkeypatch.setattr(bridge, "ROOT", tmp_path)
    candidate = tmp_path / path
    if path != "absent":
        candidate.mkdir(parents=True)
    with pytest.raises(bridge.BridgeError):
        bridge.sdk_directory(candidate, sdk)


@pytest.fixture
def handoff(record, sdk_root, monkeypatch):
    credentials = bridge.temporary(record)
    export = Mock(return_value=credentials)
    cleanup = Mock()
    calls = []

    def capture(command, **kwargs):
        calls.append((command, kwargs))
        if "inspect" in command:
            return ("sha256:" + "a" * 64).encode()
        if "ls" in command:
            return b""
        return json.dumps(bridge.reply("glue", False)).encode()

    monkeypatch.setattr(bridge, "capture", capture)
    monkeypatch.setattr(bridge, "export_login", export)
    monkeypatch.setattr(bridge, "cleanup", cleanup)
    monkeypatch.setattr(bridge, "local_docker", lambda docker: ([docker, "--context", "local"], {}))
    return SimpleNamespace(
        credentials=credentials, export=export, cleanup=cleanup, calls=calls, sdk=sdk_root
    )


def test_handoff_sends_credentials_only_on_stdin_and_clears_them(handoff):
    report = bridge.run_handoff(OWNER, "urban-geo", "glue", handoff.sdk, "aws", "docker")
    assert (
        report["live_identity_verified"] is True and report["pipeline_execution_verified"] is False
    )
    run = next(item for item in handoff.calls if "run" in item[0])
    assert KEY.encode() in run[1]["content"]
    assert all(value not in str(run[0]) + json.dumps(report) for value in (KEY, SECRET, TOKEN))
    handoff.cleanup.assert_called_once()
    assert set(handoff.credentials.session_options().values()) == {""}


@pytest.mark.parametrize("failure", ["process", "reply"])
def test_handoff_failure_cleans_owned_container_and_credential_references(
    handoff, monkeypatch, failure
):
    original = bridge.capture

    def capture(command, **kwargs):
        if "run" in command:
            if failure == "process":
                raise bridge.BridgeError("failed")
            return b'{"unexpected":"reply"}'
        return original(command, **kwargs)

    monkeypatch.setattr(bridge, "capture", capture)
    with pytest.raises(bridge.BridgeError):
        bridge.run_handoff(OWNER, "urban-geo", "glue", handoff.sdk, "aws", "docker")
    handoff.cleanup.assert_called_once()
    assert handoff.credentials.token == ""


def test_preexisting_container_name_is_not_deleted_or_given_credentials(handoff, monkeypatch):
    original = bridge.capture
    monkeypatch.setattr(
        bridge,
        "capture",
        lambda command, **kwargs: b"existing" if "ls" in command else original(command, **kwargs),
    )
    with pytest.raises(bridge.BridgeError, match="already_exists"):
        bridge.run_handoff(OWNER, "urban-geo", "glue", handoff.sdk, "aws", "docker")
    handoff.cleanup.assert_not_called()
    handoff.export.assert_not_called()


@pytest.fixture
def receiver(record, monkeypatch):
    credentials = bridge.temporary(record)
    sts = SimpleNamespace(
        get_caller_identity=Mock(
            return_value={
                "Account": OWNER,
                "Arn": f"arn:aws:sts::{OWNER}:assumed-role/ProjectAdmin/session",
            }
        )
    )
    closed = Mock()

    @contextmanager
    def owned(creds, sdk):
        assert creds is credentials
        try:
            yield {"sts": sts}
        finally:
            closed()

    monkeypatch.setattr(worker, "receive", lambda raw: (credentials, OWNER))
    monkeypatch.setattr(worker, "owned_clients", owned)
    return SimpleNamespace(credentials=credentials, sts=sts, closed=closed)


def test_receiver_checks_exact_account_and_closes_before_safe_reply(receiver):
    report = worker.probe(b"owned", "glue")
    assert report == bridge.reply("glue", False)
    receiver.sts.get_caller_identity.assert_called_once_with()
    receiver.closed.assert_called_once()
    assert receiver.credentials.token == "" and TOKEN not in json.dumps(report)


@pytest.mark.parametrize(
    "identity",
    [
        {"Account": "999999999999", "Arn": "wrong"},
        {"Account": OWNER, "Arn": f"arn:aws:iam::{OWNER}:root"},
        {"Account": OWNER, "Arn": "arn:aws:sts::999999999999:assumed-role/reader/session"},
    ],
)
def test_wrong_account_or_non_role_identity_fails_with_cleanup(receiver, identity):
    receiver.sts.get_caller_identity.return_value = identity
    with pytest.raises(bridge.BridgeError):
        worker.probe(b"owned", "glue")
    receiver.closed.assert_called_once()
    assert receiver.credentials.token == ""


def test_sdk_failure_still_closes_and_forgets_credentials(receiver):
    receiver.sts.get_caller_identity.side_effect = RuntimeError("provider failure")
    with pytest.raises(RuntimeError):
        worker.probe(b"owned", "glue")
    receiver.closed.assert_called_once()
    assert receiver.credentials.token == ""


def test_default_cli_is_plan_only_without_export_or_docker(sdk_root, monkeypatch, capsys):
    run = Mock(side_effect=AssertionError("Must not export"))
    monkeypatch.setattr(bridge, "run_handoff", run)
    monkeypatch.setattr(
        sys,
        "argv",
        ["bridge", "--expected-account-id", OWNER, "--sdk", "glue", "--sdk-root", str(sdk_root)],
    )
    bridge.main()
    assert json.loads(capsys.readouterr().out)["credential_exported"] is False
    run.assert_not_called()


@pytest.mark.parametrize(
    "endpoint",
    ["tcp://example.invalid:2376", "ssh://remote", "tcp://localhost:2375", "http://remote"],
)
def test_remote_docker_is_rejected_before_any_export(monkeypatch, endpoint):
    capture = Mock(side_effect=[b"selected", endpoint.encode()])
    monkeypatch.setattr(bridge, "capture", capture)
    with pytest.raises(bridge.BridgeError, match="local_docker_socket"):
        bridge.local_docker("docker")


@pytest.mark.parametrize(
    "endpoint", ["unix:///var/run/docker.sock", "npipe:////./pipe/dockerDesktopLinuxEngine"]
)
def test_local_context_is_pinned_and_environment_overrides_removed(monkeypatch, endpoint):
    capture = Mock(side_effect=[b"desktop-linux", endpoint.encode()])
    monkeypatch.setattr(bridge, "capture", capture)
    monkeypatch.setenv("DOCKER_HOST", "tcp://remote")
    monkeypatch.setenv("DOCKER_CONTEXT", "remote")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", SECRET)
    command, environment = bridge.local_docker("docker")
    assert command == ["docker", "--context", "desktop-linux"]
    assert not any(key.startswith(("AWS_", "DOCKER_")) for key in environment)


def test_boolean_shaped_numeric_reply_cannot_claim_success(handoff, monkeypatch):
    original = bridge.capture

    def capture(command, **kwargs):
        if "run" in command:
            return json.dumps(bridge.reply("glue", False) | {"account_matches": 1}).encode()
        return original(command, **kwargs)

    monkeypatch.setattr(bridge, "capture", capture)
    with pytest.raises(bridge.BridgeError, match="reply"):
        bridge.run_handoff(OWNER, "urban-geo", "glue", handoff.sdk, "aws", "docker")
    handoff.cleanup.assert_called_once()


def test_cleanup_removes_only_the_exact_owned_container_and_rechecks_absence(monkeypatch):
    capture = Mock(side_effect=[b"owned-id", b"removed", b""])
    monkeypatch.setattr(bridge, "capture", capture)
    name = "urban-geo-login-" + "a" * 16
    bridge.cleanup(["docker", "--context", "local"], name, {})
    assert capture.call_args_list[1].args[0] == [
        "docker",
        "--context",
        "local",
        "container",
        "rm",
        "--force",
        name,
    ]
    assert capture.call_count == 3


def test_cleanup_failure_names_owned_container_without_provider_details(monkeypatch):
    monkeypatch.setattr(bridge, "capture", Mock(side_effect=RuntimeError(SECRET)))
    name = "urban-geo-login-" + "a" * 16
    with pytest.raises(bridge.BridgeError) as error:
        bridge.cleanup(["docker"], name, {})
    assert error.value.args == ("owned_container_cleanup_unconfirmed", name)
    assert SECRET not in str(error.value)


def test_receiver_cli_suppresses_raw_sdk_errors(monkeypatch, capsys):
    monkeypatch.setattr(worker, "probe", Mock(side_effect=RuntimeError(SECRET)))
    monkeypatch.setattr(sys, "argv", ["probe", "--sdk", "glue"])
    monkeypatch.setattr(
        sys, "stdin", SimpleNamespace(buffer=SimpleNamespace(read=Mock(return_value=b"owned")))
    )
    with pytest.raises(SystemExit) as error:
        worker.main()
    output = capsys.readouterr()
    assert error.value.code == 2 and SECRET not in output.out + output.err
    assert json.loads(output.out)["error"] == "login_probe_failed"
