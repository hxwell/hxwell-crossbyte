"""Isolated Haxelib registration shared by integration and core regression runs."""
import json
from pathlib import Path
import subprocess


def local_libraries(work, root, hxwell, crossbyte, emitter, concurrent=None, haxe5=False):
    libraries = {"hxwell-crossbyte": root, "hxwell": hxwell, "crossbyte": crossbyte, "emitter": emitter}
    for name in ("hxjava", "haxe-concurrent", "uuid", "crypto", "utest"):
        libraries[name] = Path(subprocess.check_output(["haxelib", "libpath", name], cwd=root, text=True).strip())
    if haxe5:
        libraries["hx4compat"] = Path(subprocess.check_output(["haxelib", "libpath", "hx4compat"], cwd=root, text=True).strip())
    if concurrent:
        libraries["haxe-concurrent"] = concurrent.resolve()
    subprocess.run(["haxelib", "newrepo"], cwd=work, check=True, stdout=subprocess.DEVNULL)
    for name, path in libraries.items():
        folder = work / ".haxelib" / name
        folder.mkdir(parents=True)
        (folder / ".dev").write_text(str(path) + "\n")
        version = json.loads((path / "haxelib.json").read_text())["version"]
        (folder / version.replace(".", ",")).symlink_to(path, target_is_directory=True)
        (folder / ".current").write_text(version + "\n")


def revision(path):
    return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=path, text=True).strip()
