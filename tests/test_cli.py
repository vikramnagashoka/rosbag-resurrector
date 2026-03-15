"""Tests for the CLI commands."""

import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from tests.fixtures.generate_test_bags import generate_bag, BagConfig
from resurrector.cli.main import app

runner = CliRunner()


@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)


@pytest.fixture
def healthy_bag(tmp_dir):
    return generate_bag(tmp_dir / "healthy.mcap", BagConfig(duration_sec=2.0))


@pytest.fixture
def bag_dir(tmp_dir):
    bags = tmp_dir / "bags"
    bags.mkdir()
    generate_bag(bags / "test1.mcap", BagConfig(duration_sec=2.0))
    generate_bag(bags / "test2.mcap", BagConfig(duration_sec=2.0))
    return bags


class TestScanCommand:
    def test_scan_file(self, healthy_bag, tmp_dir):
        result = runner.invoke(app, [
            "scan", str(healthy_bag), "--db", str(tmp_dir / "test.db"),
        ])
        assert result.exit_code == 0
        assert "Found" in result.stdout
        assert "Indexed" in result.stdout

    def test_scan_directory(self, bag_dir, tmp_dir):
        result = runner.invoke(app, [
            "scan", str(bag_dir), "--db", str(tmp_dir / "test.db"),
        ])
        assert result.exit_code == 0
        assert "2" in result.stdout  # 2 bag files

    def test_scan_empty_dir(self, tmp_dir):
        empty = tmp_dir / "empty"
        empty.mkdir()
        result = runner.invoke(app, ["scan", str(empty)])
        assert result.exit_code == 0
        assert "No bag files found" in result.stdout


class TestInfoCommand:
    def test_info(self, healthy_bag):
        result = runner.invoke(app, ["info", str(healthy_bag)])
        assert result.exit_code == 0
        assert "healthy.mcap" in result.stdout
        assert "/imu/data" in result.stdout


class TestHealthCommand:
    def test_health_rich(self, healthy_bag):
        result = runner.invoke(app, ["health", str(healthy_bag)])
        assert result.exit_code == 0
        assert "Health" in result.stdout

    def test_health_json(self, healthy_bag, tmp_dir):
        output = tmp_dir / "report.json"
        result = runner.invoke(app, [
            "health", str(healthy_bag), "--format", "json", "--output", str(output),
        ])
        assert result.exit_code == 0
        assert output.exists()
        import json
        data = json.loads(output.read_text())
        assert isinstance(data, dict)


class TestListCommand:
    def test_list_empty(self, tmp_dir):
        result = runner.invoke(app, [
            "list", "--db", str(tmp_dir / "empty.db"),
        ])
        assert result.exit_code == 0
        assert "No bags found" in result.stdout

    def test_list_after_scan(self, bag_dir, tmp_dir):
        db = str(tmp_dir / "test.db")
        runner.invoke(app, ["scan", str(bag_dir), "--db", db])
        result = runner.invoke(app, ["list", "--db", db])
        assert result.exit_code == 0
        assert "test1.mcap" in result.stdout or "test2.mcap" in result.stdout


class TestExportCommand:
    def test_export_parquet(self, healthy_bag, tmp_dir):
        output = tmp_dir / "export"
        result = runner.invoke(app, [
            "export", str(healthy_bag),
            "--topics", "/imu/data",
            "--format", "parquet",
            "--output", str(output),
        ])
        assert result.exit_code == 0
        assert "Exported" in result.stdout
        assert (output / "imu_data.parquet").exists()

    def test_export_csv(self, healthy_bag, tmp_dir):
        output = tmp_dir / "export_csv"
        result = runner.invoke(app, [
            "export", str(healthy_bag),
            "--topics", "/imu/data",
            "--format", "csv",
            "--output", str(output),
        ])
        assert result.exit_code == 0


class TestDiffCommand:
    def test_diff(self, bag_dir):
        bags = list(bag_dir.glob("*.mcap"))
        result = runner.invoke(app, ["diff", str(bags[0]), str(bags[1])])
        assert result.exit_code == 0
        assert "Comparison" in result.stdout


class TestQuicklookCommand:
    def test_quicklook(self, healthy_bag):
        result = runner.invoke(app, ["quicklook", str(healthy_bag)])
        assert result.exit_code == 0
        assert "quicklook" in result.stdout
        assert "/imu/data" in result.stdout


class TestDatasetCommands:
    def test_dataset_create(self, tmp_dir):
        result = runner.invoke(app, [
            "dataset", "create", "test-ds", "--desc", "A test dataset",
            "--db", str(tmp_dir / "test.db"),
        ])
        assert result.exit_code == 0
        assert "Created" in result.stdout

    def test_dataset_list_empty(self, tmp_dir):
        result = runner.invoke(app, [
            "dataset", "list", "--db", str(tmp_dir / "test.db"),
        ])
        assert result.exit_code == 0
        assert "No datasets found" in result.stdout

    def test_dataset_full_workflow(self, healthy_bag, tmp_dir):
        db = str(tmp_dir / "test.db")

        # Create
        result = runner.invoke(app, [
            "dataset", "create", "myds", "--db", db,
        ])
        assert result.exit_code == 0

        # Add version
        result = runner.invoke(app, [
            "dataset", "add-version", "myds", "1.0",
            "--bag", str(healthy_bag),
            "--topic", "/imu/data",
            "--db", db,
        ])
        assert result.exit_code == 0

        # List
        result = runner.invoke(app, ["dataset", "list", "--db", db])
        assert result.exit_code == 0
        assert "myds" in result.stdout

        # Export
        output = tmp_dir / "ds_export"
        result = runner.invoke(app, [
            "dataset", "export", "myds", "1.0",
            "--output", str(output),
            "--db", db,
        ])
        assert result.exit_code == 0
        assert (output / "myds" / "1.0" / "README.md").exists()
