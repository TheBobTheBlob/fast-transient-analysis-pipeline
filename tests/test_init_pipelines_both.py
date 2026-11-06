"""Combined initialization contracts, using only temporary files and fixtures."""
import ast
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/dags"))
sys.path.insert(0, str(ROOT / "src/pipeline"))
from init_pipeline_config import (  # noqa: E402
    normalize_pipeline_branch, postprocessing_task_for_branch,
    raw_dirs_for_branch, resolve_pipeline_config,
)
from init_pipeline_products import create_product_directories, create_product_links  # noqa: E402
from stage_files import stage_pipeline_config  # noqa: E402


class CombinedInitializationTests(unittest.TestCase):
    def plan(self, root):
        dirs = {family: {kind: str(root / "raw" / family / kind) for kind in groups}
                for family, groups in raw_dirs_for_branch("both").items()}
        plan = resolve_pipeline_config({"pipeline_branch": "both"}, dirs)
        for family, config in plan["branches"].items():
            config["destination_root"] = str(root / "tdrss")
            config["inputs"] = {
                kind: str(root / "inputs" / family / Path(remote).name)
                for kind, remote in config["inputs"].items()
            }
            for source in config["inputs"].values():
                p = Path(source)
                p.parent.mkdir(parents=True, exist_ok=True)
                p.write_bytes(b"local test data")
        return plan

    def test_both_selects_two_independent_resolved_configs(self):
        self.assertEqual(normalize_pipeline_branch("BOTH"), "both")
        config = resolve_pipeline_config({"pipeline_branch": "both", "destination": "lcurve", "eps_time": 12})
        ged, bgo = config["branches"]["GeD"], config["branches"]["BGO"]
        self.assertEqual(ged, resolve_pipeline_config({"pipeline_branch": "GeD", "destination": "lcurve", "eps_time": 12}))
        self.assertEqual(bgo, resolve_pipeline_config({"pipeline_branch": "BGO", "destination": "lcurve", "eps_time": 12}))
        self.assertNotEqual(ged["dirs"]["orientation"], bgo["dirs"]["orientation"])
        self.assertEqual(bgo["destination"], "tdrss")
        self.assertEqual(ged["destination"], "lcurve")

    def test_both_respects_each_familys_overrides(self):
        plan = resolve_pipeline_config({"pipeline_branch": "both", "orientation_path": "/ged/ged.ori",
            "bgo_orientation_path": "/bgo/bgo.ori", "lightcurve_path": "/bgo/new.npz"})
        self.assertEqual(plan["branches"]["GeD"]["inputs"]["orientation"], "/ged/ged.ori")
        self.assertEqual(plan["branches"]["BGO"]["inputs"]["orientation"], "/bgo/bgo.ori")
        self.assertEqual(plan["branches"]["BGO"]["inputs"]["lightcurve"], "/bgo/new.npz")

    def test_combined_branch_selects_both_postprocessing_paths(self):
        self.assertEqual(postprocessing_task_for_branch("both"), ["background_cut", "bgo_staging_complete"])

    def test_malformed_combined_dirs_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "GeD and BGO"):
            resolve_pipeline_config({"pipeline_branch": "both"}, {"BGO": {}})

    def test_both_validation_finishes_before_any_download(self):
        with tempfile.TemporaryDirectory() as d:
            plan = self.plan(Path(d))
            plan["branches"]["BGO"]["dirs"].pop("lightcurve")
            download = Mock()
            with self.assertRaises(ValueError):
                stage_pipeline_config(plan, downloader=download)
            download.assert_not_called()

    def test_both_rejects_cross_family_raw_target_collisions(self):
        with tempfile.TemporaryDirectory() as d:
            plan = self.plan(Path(d))
            for family in ("GeD", "BGO"):
                plan["branches"][family]["inputs"]["orientation"] = "/example/same.fits"
                plan["branches"][family]["dirs"]["orientation"] = str(Path(d) / "same-raw")
            with self.assertRaisesRegex(ValueError, "same raw file"):
                stage_pipeline_config(plan, downloader=Mock())

    def test_combined_staging_links_and_manifests_keep_the_families_separate(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            plan = self.plan(root)
            # This fixture validates orchestration, not scientific FITS contents.
            def download(source, directory, **kwargs):
                directory.mkdir(parents=True, exist_ok=True)
                target = directory / Path(source).name
                target.write_bytes(Path(source).read_bytes())
                return str(target)
            staged = stage_pipeline_config(plan, downloader=download)
            products = create_product_directories(plan)
            self.assertNotEqual(products["products_dirs"]["GeD"], products["products_dirs"]["BGO"])
            links = create_product_links(plan, staged, products)
            self.assertEqual(set(links["GeD"]), {"source", "background", "orientation", "response"})
            self.assertEqual(set(links["BGO"]), {"lightcurve", "soft_lut", "medium_lut", "hard_lut", "orientation"})
            for family in ("GeD", "BGO"):
                directory = Path(products["products_dirs"][family])
                manifest = json.loads((directory / "initialization_manifest.json").read_text())
                self.assertEqual(manifest["pipeline_branch"], family)
                for path in links[family].values():
                    self.assertTrue(Path(path).is_symlink())
                    self.assertTrue(Path(path).is_file())
            # Retrying publication reuses identical links; it cannot change data.
            self.assertEqual(create_product_links(plan, staged, products), links)

    def test_single_branch_xcom_shapes_are_preserved(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d).resolve()
            plan = self.plan(root)["branches"]["BGO"]
            staged = stage_pipeline_config(plan, downloader=lambda source, directory, **kw: source)
            self.assertEqual(set(staged), set(plan["inputs"]))
            products = create_product_directories(plan)
            self.assertEqual(set(products), {"products_dir"})
            self.assertEqual(set(create_product_links(plan, staged, products)), set(plan["inputs"]))

    def test_cli_accepts_both_config_and_preserves_legacy_inputs_mode(self):
        with tempfile.TemporaryDirectory() as d:
            plan = self.plan(Path(d).resolve())
            # Avoid scientific validation; staging a local raw LUT needs no dependencies.
            for family, cfg in plan["branches"].items():
                source = Path(d) / f"{family}-fixture.pkl"
                source.write_bytes(b"local fixture")
                cfg["inputs"] = {"soft_lut": str(source)}
                cfg["dirs"] = {"soft_lut": str(Path(d) / family)}
            script = str(ROOT / "src/pipeline/stage_files.py")
            result = subprocess.run([sys.executable, script, "--config", json.dumps(plan)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(set(json.loads(result.stdout.splitlines()[-1])), {"GeD", "BGO"})
            cfg = plan["branches"]["BGO"]
            legacy = subprocess.run([sys.executable, script, "--inputs", json.dumps(cfg["inputs"]),
                "--dirs", json.dumps(cfg["dirs"])], capture_output=True, text=True)
            self.assertEqual(legacy.returncode, 0, legacy.stderr)
            self.assertEqual(set(json.loads(legacy.stdout.splitlines()[-1])), {"soft_lut"})

    def test_background_templates_select_only_ged_inputs_in_both_modes(self):
        try:
            from jinja2 import Environment, StrictUndefined
        except ImportError:
            self.skipTest("Jinja2 not installed")
        tree = ast.parse((ROOT / "src/dags/cosipipe_initpipeline.py").read_text())
        assignment = next(n for n in ast.walk(tree) if isinstance(n, ast.Assign)
            and any(isinstance(t, ast.Name) and t.id == "t_bkgcut" for t in n.targets))
        env_node = next(k.value for k in assignment.value.keywords if k.arg == "env")
        templates = {ast.literal_eval(k): ast.literal_eval(v) for k, v in zip(env_node.keys, env_node.values)
                     if isinstance(v, ast.Constant) and isinstance(v.value, str)}
        for combined in (False, True):
            ged = {"pipeline_branch": "GeD", "eps_time": 42}
            links = {"source": "/ged/source", "background": "/ged/background"}
            xcom = {"resolve_config": {"pipeline_branch": "both", "branches": {"GeD": ged}} if combined else ged,
                    "create_symlinks": {"GeD": links, "BGO": {"orientation": "/bgo"}} if combined else links}
            ti = Mock(xcom_pull=lambda task_ids: xcom[task_ids])
            rendered = {key: Environment(undefined=StrictUndefined).from_string(templates[key]).render(ti=ti)
                        for key in ("SOURCE_PATH", "BACKGROUND_PATH", "EPS_TIME")}
            self.assertEqual(rendered, {"SOURCE_PATH": "/ged/source", "BACKGROUND_PATH": "/ged/background", "EPS_TIME": "42"})


if __name__ == "__main__":
    unittest.main()
