#!/usr/bin/env python3
"""Run HxWell's complete unit/security suite with Crossbyte's classpaths active."""
import argparse
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from environment import local_libraries, revision

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("hxwell", "crossbyte", "emitter"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--haxe", default="haxe")
    parser.add_argument("--target", choices=["jvm", "neko"], default="jvm")
    parser.add_argument("--concurrent", type=Path)
    args = parser.parse_args()
    version = subprocess.check_output([args.haxe, "--version"], text=True).strip()
    if version not in ("4.3.7", "5.0.0-preview.1") or (version.startswith("5.") and args.target != "jvm"):
        raise ValueError("Use Haxe 4.3.7, or Haxe 5.0.0-preview.1 on JVM")
    hxwell, crossbyte, emitter = args.hxwell.resolve(), args.crossbyte.resolve(), args.emitter.resolve()
    (ROOT / "out").mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=f"core-{args.target}-", dir=ROOT / "out"))
    (work / "resources/views").mkdir(parents=True)
    (work / "out").mkdir()
    shutil.copytree(hxwell / "tests/unit/public", work / "public")
    shutil.copy2(hxwell / "tests/unit/secrets.txt", work / "secrets.txt")
    local_libraries(work, ROOT, hxwell, crossbyte, emitter, args.concurrent, haxe5=version.startswith("5."))
    hxml = (hxwell / "tests/unit/build.hxml").read_text()
    hxml = hxml.replace("-cp ../../src", f"-cp {hxwell}/src").replace("-cp src", f"-cp {hxwell}/tests/unit/src")
    hxml += (f"\n-cp {crossbyte}/src\n-cp {emitter}/src\n"
             "--macro crossbyte._internal.macro.HostPlatform.define()\n"
             "--macro crossbyte._internal.macro.StdOverrides.use()\n")
    hxml += f"-cp {ROOT}/src\n--macro hx.well.http.driver.crossbyte.macro.Compatibility.check()\n"
    if version.startswith("5."):
        hxml += "-lib hx4compat\n"
    if args.target == "jvm":
        hxml += "-lib hxjava\n-D security_socket_tests\n--jvm test.jar\n"
        command = ["java", "-Xmx128m", "-jar", "test.jar"]
    else:
        hxml += "--neko test.n\n"
        command = ["neko", "test.n"]
    (work / "build.hxml").write_text(hxml)
    report = {"success": False, "target": args.target, "haxe": version, "workdir": str(work), "hxwellRevision": revision(hxwell), "crossbyteRevision": revision(crossbyte)}
    try:
        with (work / "build.log").open("w") as log:
            subprocess.run([args.haxe, "build.hxml"], cwd=work, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=180)
        with (work / "test.log").open("w") as log:
            subprocess.run(command, cwd=work, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=120)
        output = (work / "test.log").read_text()
        report["success"] = "ALL TESTS OK (success: true)" in output
        for label in ("assertations", "successes", "errors", "failures"):
            match = re.search(rf"(?m)^{label}:\s*(\d+)", output)
            if match:
                report[label] = int(match[1])
        if not report["success"]:
            raise AssertionError(f"HxWell core tests failed; see {work / 'test.log'}")
    finally:
        (ROOT / f"out/core-{args.target}.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
