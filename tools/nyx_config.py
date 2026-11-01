#!/usr/bin/env python3
"""
Promote the NYX platform configuration between instances.

The API exposes the design of an instance (screens, index rules, privileges, filters,
translations, datasources) as one JSON bundle, and can plan or apply the import of such a bundle.
This script is the thin client for that: export a bundle, review the plan, apply it.

    export NYX_TOKEN=...                      # an admin token, never pass it on the command line
    nyx_config.py export --url https://dev.example.com -o config/bundle.json
    nyx_config.py plan   config/bundle.json --url https://prod.example.com
    nyx_config.py apply  config/bundle.json --url https://prod.example.com --prune --yes

`plan` and `apply` are the same request; `apply` without `--yes` prints the plan and exits with
code 2, so a pipeline cannot write by accident. Use --prune to delete documents the bundle does
not contain (a screen removed in dev disappears in prod).

Only the standard library is used: this must run on an operator laptop without the API's
dependencies installed.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sources"))

from config_bundle import describe_plan, validate_bundle  # noqa: E402


def call(url, token, path, payload=None, params=None):
    """Call the API and return the decoded body, raising on a transport or API error."""
    full = url.rstrip("/") + "/api/v1/" + path
    query = {"token": token}
    if params:
        query.update({k: v for k, v in params.items() if v is not None})
    full += "?" + urllib.parse.urlencode(query)

    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(full, data=data, headers=headers, method="POST" if data else "GET")
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        raise SystemExit(f"HTTP {error.code} from {path}: {error.read().decode('utf-8', 'replace')[:400]}")
    except urllib.error.URLError as error:
        raise SystemExit(f"cannot reach {url}: {error.reason}")

    if body.get("error"):
        details = body.get("details") or []
        message = body["error"] + (": " + json.dumps(details) if details else "")
        raise SystemExit(message)
    return body


def token_from(args):
    token = args.token or os.environ.get("NYX_TOKEN")
    if not token:
        raise SystemExit("no token: set NYX_TOKEN or pass --token (an admin token is required)")
    return token


def print_summary(summary):
    print(
        "create={create} update={update} unchanged={unchanged} delete={delete}".format(**summary)
    )


def command_export(args):
    body = call(args.url, token_from(args), "config/export", params={"indices": args.indices})
    bundle = body["bundle"]
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(bundle, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(f"wrote {output} — {json.dumps(body.get('counts', {}))}")
    else:
        json.dump(bundle, sys.stdout, indent=2, sort_keys=True)
        sys.stdout.write("\n")


def command_plan(args, apply=False):
    bundle = json.loads(Path(args.bundle).read_text(encoding="utf-8"))
    problems = validate_bundle(bundle)
    if problems:
        raise SystemExit("the bundle is not importable:\n  " + "\n  ".join(problems))

    result = call(
        args.url,
        token_from(args),
        "config/import",
        payload=bundle,
        params={"apply": "1" if apply else "0", "prune": "1" if args.prune else "0"},
    )

    print(describe_plan(result))
    print()
    print_summary(result["summary"])
    if apply and result.get("applied"):
        print(f"applied {result.get('operations')} operation(s) at {result.get('appliedAt')}")
    elif apply:
        print("nothing to apply: the target already matches the bundle")
    return result


def command_plan_show(args):
    command_plan(args, apply=False)
    return 0


def command_apply(args):
    if not args.yes:
        command_plan(args, apply=False)
        print("\ndry run only — re-run with --yes to write the changes")
        return 2
    command_plan(args, apply=True)
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description="Promote the NYX platform configuration between instances.")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--url", required=True, help="base URL of the target instance, e.g. https://nyx.example.com")
    common.add_argument("--token", help="admin token (prefer the NYX_TOKEN environment variable)")

    subparsers = parser.add_subparsers(dest="command", required=True)

    export = subparsers.add_parser("export", parents=[common], help="download the configuration bundle")
    export.add_argument("-o", "--output", help="file to write (default: stdout)")
    export.add_argument("--indices", help="comma separated subset of the design indices")
    export.set_defaults(func=command_export)

    plan = subparsers.add_parser("plan", parents=[common], help="show what importing a bundle would change")
    plan.add_argument("bundle", help="bundle written by export")
    plan.add_argument("--prune", action="store_true", help="also plan the deletions")
    plan.set_defaults(func=command_plan_show)

    apply = subparsers.add_parser("apply", parents=[common], help="import a bundle (needs --yes)")
    apply.add_argument("bundle", help="bundle written by export")
    apply.add_argument("--prune", action="store_true", help="also delete what the bundle omits")
    apply.add_argument("--yes", action="store_true", help="actually write the changes")
    apply.set_defaults(func=command_apply)

    args = parser.parse_args(argv)
    result = args.func(args)
    return result if isinstance(result, int) else 0


if __name__ == "__main__":
    sys.exit(main())
