"""Products layout for single-family and combined FasTP initialization."""
from datetime import datetime
import json
from pathlib import Path
import re
from typing import Dict

def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def next_run_products_dir(base_dest: Path) -> Path:
    # Determines the next run directory based on date and incremental index
    now = datetime.now()
    yyyy_mm = now.strftime("%Y_%m")
    yymmdd = now.strftime("%y%m%d")
    base_month_dir = ensure_dir(base_dest / yyyy_mm)
    pat = re.compile(rf"^{yymmdd}(\d{{3}})$")
    max_idx = -1
    for d in base_month_dir.iterdir():
        if d.is_dir() and pat.match(d.name):
            max_idx = max(max_idx, int(pat.match(d.name).group(1)))
    index = max_idx + 1
    while True:
        run_dir = base_month_dir / f"{yymmdd}{index:03d}"
        try:
            run_dir.mkdir()
            break
        except FileExistsError:
            index += 1
    return ensure_dir(run_dir / "products")


def make_symlinks(target_dir: Path, files_by_kind: Dict[str, str]) -> Dict[str, str]:
    """Create symlinks in target_dir for all paths in files_by_kind."""
    result = {}
    links_by_name = {}
    for kind, paths in files_by_kind.items():
        link = target_dir / Path(paths).name
        if link.name in links_by_name:
            raise ValueError(
                f"[init_pipelines] inputs {links_by_name[link.name]!r} and {kind!r} "
                f"have the same basename {link.name!r}; refusing to overwrite a product"
            )
        links_by_name[link.name] = kind
        if link.is_symlink() and link.resolve() != Path(paths).resolve():
            raise FileExistsError(f"[init_pipelines] existing symlink has a different target: {link}")
        if link.exists() and not link.is_symlink():
            raise FileExistsError(f"[init_pipelines] product path already exists: {link}")
        if not link.exists() and not link.is_symlink():
            link.symlink_to(paths)
            print(f"[symlink] {link} -> {paths}")
        result[kind] = str(link)
    return result


def create_product_directories(config):
    """Preserve the single-branch XCom; combined mode returns separate paths."""
    if config["pipeline_branch"] != "both":
        return {"products_dir": str(next_run_products_dir(Path(config["destination_root"])))}
    return {"products_dirs": {
        family: str(next_run_products_dir(Path(plan["destination_root"])))
        for family, plan in config["branches"].items()
    }}


def create_product_links(config, staged, products):
    """Validate all branch outputs before linking and record their families."""
    combined = config["pipeline_branch"] == "both"
    plans = config["branches"] if combined else {config["pipeline_branch"]: config}
    if combined and (not isinstance(staged, dict) or set(staged) != set(plans)):
        raise ValueError("[init_pipelines:both] staged branches must be GeD and BGO")
    # Preflight the complete staging result before publishing either set.
    for family, plan in plans.items():
        files = staged[family] if combined else staged
        if not isinstance(files, dict) or set(files) != set(plan["inputs"]):
            raise ValueError(f"[init_pipelines:{family}] staged keys must be {sorted(plan['inputs'])}")
        names = [Path(value).name for value in files.values()]
        if len(set(names)) != len(names):
            raise ValueError(f"[init_pipelines:{family}] inputs have the same basename")
    results = {}
    for family in plans:
        files = staged[family] if combined else staged
        directory = Path(products["products_dirs"][family] if combined else products["products_dir"])
        results[family] = make_symlinks(directory, files)
        # Descriptive metadata; COSIDAG currently does not use it for routing.
        manifest = {"schema_version": 1, "pipeline_branch": family, "input_paths": files}
        temporary = directory / ".initialization_manifest.json.tmp"
        temporary.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        temporary.replace(directory / "initialization_manifest.json")
    return results if combined else results[config["pipeline_branch"]]
