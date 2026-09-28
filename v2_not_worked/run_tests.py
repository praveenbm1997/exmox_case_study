"""Pytest runner for Databricks job task.

Runs the 17 transformation tests as a gate before the pipeline.
If any test fails, the job task fails and downstream tasks are skipped.
"""
import sys, os, subprocess

# Ensure the pipeline directory (where transforms.py lives) is on the path
_pipeline_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _pipeline_dir)

# Install pytest if not available
try:
    import pytest
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pytest", "-q"])
    import pytest

# Run the tests and exit with pytest's exit code
_exit = pytest.main([
    "-v", "--tb=short", "--no-header",
    os.path.join(_pipeline_dir, "tests", "test_transforms.py"),
])
sys.exit(_exit)
