"""Inspect an isolated bundle installation; use no imports from the working checkout."""

import argparse
import importlib
import importlib.metadata
import json
import platform
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", type=Path, required=True)
    parser.add_argument("--project-root", type=Path, required=True)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--provided-root", type=Path, required=True)
    args = parser.parse_args()
    site, root = args.site.resolve(), args.project_root.resolve()
    expected_environment = args.provided_root.parent / "validation-venv"
    assert Path(sys.prefix).resolve() == expected_environment.resolve()
    assert "include-system-site-packages = false" in (Path(sys.prefix) / "pyvenv.cfg").read_text(
        encoding="utf-8"
    )
    sys.path = [p for p in sys.path if p and not Path(p).resolve().is_relative_to(root)]
    sys.path.insert(0, str(args.provided_root.resolve()))
    sys.path.insert(0, str(site))
    inputs = json.loads(args.inputs.read_text(encoding="utf-8"))
    origins = {}
    for name, expected in inputs["pins"].items():
        distribution = importlib.metadata.distribution(name)
        assert distribution.version == expected, (name, distribution.version, expected)
        assert Path(distribution.locate_file("")).resolve() == site, name
        module_name = {"apache-sedona": "sedona", "python-dateutil": "dateutil"}.get(
            name, name.replace("-", "_")
        )
        module = importlib.import_module(module_name)
        assert Path(module.__file__).resolve().is_relative_to(site), name
        origins[name] = Path(module.__file__).resolve().relative_to(site).as_posix()
    for name, expected in inputs["provided"].items():
        module = importlib.import_module(name)
        assert module.__version__ == expected, name
        assert not Path(module.__file__).resolve().is_relative_to(site), name
        assert (
            Path(module.__file__).resolve().is_relative_to((args.provided_root / name).resolve())
        ), name
    from urban_geography.gold import load_sql_models, sql_model_hashes

    assert sql_model_hashes(load_sql_models()) == inputs["sql_sha256"]
    import numpy as np
    import pandas as pd
    import pyarrow as pa
    from pyproj import Transformer
    from shapely.geometry import Point

    assert np.array([1, 2]).sum() == 3
    assert pd.DataFrame({"x": [1, 2]})["x"].sum() == 3
    assert pa.table({"x": [1, None]}).to_pylist() == [{"x": 1}, {"x": None}]
    projected = Transformer.from_crs(4326, 3763, always_xy=True).transform(-8.425, 40.205)
    assert all(abs(value) < 100_000 for value in projected)
    assert Point(0, 0).distance(Point(3, 4)) == 5
    import pyspark

    assert pyspark.SparkContext._active_spark_context is None
    print(
        json.dumps(
            {
                "all_package_versions_match": True,
                "installed_module_origins": origins,
                "provided_spark_and_py4j_preserved": True,
                "sql_resources_match": True,
                "native_library_operations_pass": True,
                "spark_started": False,
                "fresh_venv_without_system_site_packages": True,
                "validation_python": platform.python_version(),
                "validation_machine": platform.machine(),
                "validation_libc": platform.libc_ver(),
            }
        )
    )


if __name__ == "__main__":
    main()
