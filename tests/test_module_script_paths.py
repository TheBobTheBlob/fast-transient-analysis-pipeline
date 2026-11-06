"""Module-owned initializer scripts, including symlinked Airflow installs."""
import ast
import importlib.util
from pathlib import Path
import shutil
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "src/dags/init_pipeline_config.py"


def load_config(path):
    spec = importlib.util.spec_from_file_location("script_path_config", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ModuleScriptPathTests(unittest.TestCase):
    def test_checkout_scripts_exist_and_belong_to_the_module(self):
        config = load_config(CONFIG)
        for name, path in (("stage_files.py", config.STAGE_SCRIPT),
                           ("bkg_cut.py", config.BKG_CUT_SCRIPT)):
            with self.subTest(script=name):
                self.assertEqual(Path(path), ROOT / "src/pipeline" / name)
                self.assertTrue(Path(path).is_file())

    def test_renamed_module_loaded_through_cfmodule_finds_its_own_scripts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            module = root / "modules_pool" / "renamed-fastp"
            dags = module / "src/dags"
            pipeline = module / "src/pipeline"
            dags.mkdir(parents=True)
            pipeline.mkdir()
            shutil.copyfile(CONFIG, dags / CONFIG.name)
            for name in ("stage_files.py", "bkg_cut.py"):
                (pipeline / name).write_text("# module-owned script\n")
            installed_dags = root / "airflow/dags"
            installed_dags.mkdir(parents=True)
            link = installed_dags / "renamed-fastp.cfmodule"
            link.symlink_to(dags, target_is_directory=True)
            config = load_config(link / CONFIG.name)
            self.assertEqual(Path(config.STAGE_SCRIPT), pipeline / "stage_files.py")
            self.assertEqual(Path(config.BKG_CUT_SCRIPT), pipeline / "bkg_cut.py")
            self.assertTrue(Path(config.STAGE_SCRIPT).is_file())
            self.assertTrue(Path(config.BKG_CUT_SCRIPT).is_file())
            self.assertFalse((root / "airflow/pipeline").exists())

    def test_initializer_imports_paths_without_overriding_them(self):
        tree = ast.parse((ROOT / "src/dags/cosipipe_initpipeline.py").read_text())
        imported = {alias.name for node in tree.body
                    if isinstance(node, ast.ImportFrom) and node.module == "init_pipeline_config"
                    for alias in node.names}
        self.assertTrue({"STAGE_SCRIPT", "BKG_CUT_SCRIPT"}.issubset(imported))
        assigned = {target.id for node in tree.body if isinstance(node, ast.Assign)
                    for target in node.targets if isinstance(target, ast.Name)}
        self.assertTrue({"STAGE_SCRIPT", "BKG_CUT_SCRIPT"}.isdisjoint(assigned))

    def test_no_dag_invokes_an_unqualified_global_pipeline_script(self):
        for path in (ROOT / "src/dags").glob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Constant) and isinstance(node.value, str):
                    value = node.value
                    prefix = "/home/gamma/airflow/pipeline/"
                    if value.startswith(prefix) and value.endswith(".py"):
                        with self.subTest(file=path.name, value=value):
                            self.assertIn("/", value[len(prefix):])


if __name__ == "__main__":
    unittest.main()
