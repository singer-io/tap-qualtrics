import ast
import unittest
from pathlib import Path

from tap_qualtrics.schema import get_schemas
from tap_qualtrics.streams import STREAMS


class TestStreamContractConsistency(unittest.TestCase):
    """CI guardrails for stream-name parity across registry, schemas, and test metadata."""

    @staticmethod
    def _expected_metadata_stream_names():
        base_path = Path(__file__).resolve().parent / "base.py"
        module = ast.parse(base_path.read_text(encoding="utf-8"))
        for node in module.body:
            if not isinstance(node, ast.ClassDef) or node.name != "QualtricsBaseTest":
                continue
            for item in node.body:
                if not isinstance(item, ast.FunctionDef) or item.name != "expected_metadata":
                    continue
                for statement in item.body:
                    if isinstance(statement, ast.Return) and isinstance(statement.value, ast.Dict):
                        return {
                            key.value
                            for key in statement.value.keys
                            if isinstance(key, ast.Constant) and isinstance(key.value, str)
                        }
        raise AssertionError("Could not parse stream names from tests/base.py expected_metadata().")

    @staticmethod
    def _schema_file_stream_names():
        schema_dir = Path(__file__).resolve().parents[1] / "tap_qualtrics" / "schemas"
        return {path.stem for path in schema_dir.glob("*.json")}

    def test_registry_base_metadata_and_schema_files_match(self):
        registry_streams = set(STREAMS)
        expected_metadata_streams = self._expected_metadata_stream_names()
        schema_file_streams = self._schema_file_stream_names()

        self.assertSetEqual(
            registry_streams,
            expected_metadata_streams,
            msg=(
                "Stream drift between tap_qualtrics.streams.STREAMS and "
                "tests/base.py expected_metadata()."
            ),
        )
        self.assertSetEqual(
            registry_streams,
            schema_file_streams,
            msg=(
                "Stream drift between tap_qualtrics.streams.STREAMS and "
                "tap_qualtrics/schemas/*.json."
            ),
        )

    def test_get_schemas_and_field_metadata_match_registry(self):
        schemas, field_metadata = get_schemas()
        registry_streams = set(STREAMS)

        self.assertSetEqual(
            registry_streams,
            set(schemas),
            msg="Stream drift between STREAMS and get_schemas() schema keys.",
        )
        self.assertSetEqual(
            registry_streams,
            set(field_metadata),
            msg="Stream drift between STREAMS and get_schemas() metadata keys.",
        )


if __name__ == "__main__":
    unittest.main()
