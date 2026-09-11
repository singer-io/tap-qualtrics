"""Extended tests for tap_qualtrics/discover.py."""
import unittest
from unittest.mock import MagicMock, patch

from tap_qualtrics.discover import (
    _apply_access_checks,
    _fetch_sample_record,
    _prune_inaccessible_children,
    _topological_order,
    _add_dynamic_entries,
    discover,
)
from tap_qualtrics.exceptions import QualtricsForbiddenError, QualtricsError


# ---------------------------------------------------------------------------
# _topological_order
# ---------------------------------------------------------------------------

class TestTopologicalOrder(unittest.TestCase):

    def test_parent_before_child(self):
        streams = {
            "surveys": type("S", (), {"parent": None}),
            "survey_quotas": type("SQ", (), {"parent": "surveys"}),
        }
        order = _topological_order({"surveys", "survey_quotas"}, streams)
        self.assertLess(order.index("surveys"), order.index("survey_quotas"))

    def test_no_parent(self):
        streams = {"users": type("U", (), {"parent": None})}
        order = _topological_order({"users"}, streams)
        self.assertEqual(order, ["users"])

    def test_missing_parent_still_orders(self):
        # child refers to a parent not in the names set
        streams = {
            "child": type("C", (), {"parent": "missing_parent"}),
        }
        order = _topological_order({"child"}, streams)
        self.assertIn("child", order)

    def test_multiple_roots(self):
        streams = {
            "users": type("U", (), {"parent": None}),
            "groups": type("G", (), {"parent": None}),
        }
        order = _topological_order({"users", "groups"}, streams)
        self.assertIn("users", order)
        self.assertIn("groups", order)

    def test_no_duplicate_in_order(self):
        streams = {
            "surveys": type("S", (), {"parent": None}),
            "distributions": type("D", (), {"parent": "surveys"}),
            "distribution_history": type("DH", (), {"parent": "distributions"}),
        }
        order = _topological_order({"surveys", "distributions", "distribution_history"}, streams)
        self.assertEqual(len(order), len(set(order)))


# ---------------------------------------------------------------------------
# _fetch_sample_record
# ---------------------------------------------------------------------------

class TestFetchSampleRecord(unittest.TestCase):

    def test_returns_first_element(self):
        client = MagicMock()
        client.get.return_value = {"result": {"elements": [{"id": "U1"}]}}
        result = _fetch_sample_record(client, "users")
        self.assertEqual(result, {"id": "U1"})

    def test_empty_elements_returns_none(self):
        client = MagicMock()
        client.get.return_value = {"result": {"elements": []}}
        result = _fetch_sample_record(client, "users")
        self.assertIsNone(result)

    def test_exception_returns_none(self):
        client = MagicMock()
        client.get.side_effect = QualtricsError("error")
        result = _fetch_sample_record(client, "users")
        self.assertIsNone(result)

    def test_result_key_fallback(self):
        client = MagicMock()
        client.get.return_value = {"result": {"result": [{"id": "U1"}]}}
        result = _fetch_sample_record(client, "some-endpoint")
        self.assertEqual(result, {"id": "U1"})


# ---------------------------------------------------------------------------
# _apply_access_checks (extended)
# ---------------------------------------------------------------------------

class TestApplyAccessChecksExtended(unittest.TestCase):

    def _make_schemas(self, *names):
        schemas = {n: {"type": "object", "properties": {}} for n in names}
        fm = {n: [] for n in names}
        return schemas, fm

    def _make_stream_cls(self, accessible=True, parent=None, path="res"):
        inst = MagicMock()
        inst.check_access.return_value = accessible
        inst._make_probe_path.return_value = path
        inst._enrich_sample.side_effect = lambda s, p: s
        cls = MagicMock(return_value=inst)
        cls.parent = parent
        return cls

    def test_all_accessible_no_removal(self):
        schemas, fm = self._make_schemas("users", "groups")
        streams = {
            "users": self._make_stream_cls(accessible=True),
            "groups": self._make_stream_cls(accessible=True),
        }
        with patch("tap_qualtrics.discover.STREAMS", streams):
            _apply_access_checks(MagicMock(), schemas, fm)
        self.assertIn("users", schemas)
        self.assertIn("groups", schemas)

    def test_parent_sample_fetched_for_child_probe(self):
        schemas, fm = self._make_schemas("surveys", "distributions")
        client = MagicMock()
        client.get.return_value = {"result": {"elements": [{"id": "SV_1"}]}}

        inst_surveys = MagicMock()
        inst_surveys.check_access.return_value = True
        inst_surveys._make_probe_path.return_value = "surveys"
        inst_surveys._enrich_sample.side_effect = lambda s, p: s
        surveys_cls = MagicMock(return_value=inst_surveys)
        surveys_cls.parent = None

        inst_dist = MagicMock()
        inst_dist.check_access.return_value = True
        inst_dist._make_probe_path.return_value = "distributions"
        inst_dist._enrich_sample.side_effect = lambda s, p: s
        dist_cls = MagicMock(return_value=inst_dist)
        dist_cls.parent = "surveys"

        with patch("tap_qualtrics.discover.STREAMS", {"surveys": surveys_cls, "distributions": dist_cls}):
            with patch("tap_qualtrics.discover._fetch_sample_record", return_value={"id": "SV_1"}):
                _apply_access_checks(client, schemas, fm)

        self.assertIn("surveys", schemas)
        self.assertIn("distributions", schemas)

    def test_raises_forbidden_when_all_inaccessible(self):
        schemas, fm = self._make_schemas("users")
        streams = {"users": self._make_stream_cls(accessible=False)}
        with patch("tap_qualtrics.discover.STREAMS", streams):
            with self.assertRaises(QualtricsForbiddenError):
                _apply_access_checks(MagicMock(), schemas, fm)

    def test_child_skipped_when_parent_inaccessible(self):
        """Cover line 71: continue when parent_name in inaccessible."""
        schemas, fm = self._make_schemas("surveys", "distributions")
        surveys_cls = self._make_stream_cls(accessible=False, path="surveys")
        surveys_cls.parent = None

        dist_cls = self._make_stream_cls(accessible=True, path="distributions")
        dist_cls.parent = "surveys"

        streams = {"surveys": surveys_cls, "distributions": dist_cls}
        with patch("tap_qualtrics.discover.STREAMS", streams):
            with patch("tap_qualtrics.discover._topological_order", return_value=["surveys", "distributions"]):
                with self.assertRaises(QualtricsForbiddenError):
                    _apply_access_checks(MagicMock(), schemas, fm)
        # distributions should have been pruned because surveys was inaccessible
        self.assertNotIn("distributions", schemas)

    def test_probe_path_none_for_child_without_parent_sample(self):
        """Cover line 89: probe_path=None when stream has parent but parent sample unavailable."""
        schemas, fm = self._make_schemas("surveys", "distributions")
        client = MagicMock()

        surveys_inst = MagicMock()
        surveys_inst.check_access.return_value = True
        surveys_inst._make_probe_path.return_value = "surveys"
        surveys_inst._enrich_sample.side_effect = lambda s, p: s
        surveys_cls = MagicMock(return_value=surveys_inst)
        surveys_cls.parent = None

        dist_inst = MagicMock()
        dist_inst.check_access.return_value = True
        dist_inst._make_probe_path.return_value = "distributions"
        dist_inst._enrich_sample.side_effect = lambda s, p: s
        dist_cls = MagicMock(return_value=dist_inst)
        dist_cls.parent = "surveys"

        streams = {"surveys": surveys_cls, "distributions": dist_cls}
        with patch("tap_qualtrics.discover.STREAMS", streams):
            with patch("tap_qualtrics.discover._topological_order", return_value=["surveys", "distributions"]):
                # Return None from _fetch_sample_record so parent_samples is never populated
                with patch("tap_qualtrics.discover._fetch_sample_record", return_value=None):
                    _apply_access_checks(client, schemas, fm)
        # Both should remain accessible since child's check_access returned True
        self.assertIn("surveys", schemas)
        self.assertIn("distributions", schemas)


# ---------------------------------------------------------------------------
# _add_dynamic_entries
# ---------------------------------------------------------------------------

class TestAddDynamicEntries(unittest.TestCase):

    def test_adds_entries_to_catalog(self):
        from singer.catalog import Catalog
        catalog = Catalog([])
        client = MagicMock()

        with patch("tap_qualtrics.discover.AuditExport") as MockAudit:
            MockAudit.tap_stream_id = "audit_export"
            MockAudit.discover_dynamic_entries.return_value = (
                [("audit_export__login", {"type": "object", "properties": {}}, ["id"])],
                [],
            )
            with patch("tap_qualtrics.discover.SurveyResponseExport") as MockSurvey:
                MockSurvey.tap_stream_id = "survey_response_export"
                MockSurvey.discover_dynamic_entries.return_value = ([], [])
                _add_dynamic_entries(client, catalog)

        stream_ids = [s.tap_stream_id for s in catalog.streams]
        self.assertIn("audit_export__login", stream_ids)

    def test_qualtrics_error_skips_stream(self):
        from singer.catalog import Catalog
        catalog = Catalog([])
        client = MagicMock()

        with patch("tap_qualtrics.discover.AuditExport") as MockAudit:
            MockAudit.tap_stream_id = "audit_export"
            MockAudit.discover_dynamic_entries.side_effect = QualtricsError("error")
            with patch("tap_qualtrics.discover.SurveyResponseExport") as MockSurvey:
                MockSurvey.tap_stream_id = "survey_response_export"
                MockSurvey.discover_dynamic_entries.return_value = ([], [])
                _add_dynamic_entries(client, catalog)

        self.assertEqual(len(catalog.streams), 0)


# ---------------------------------------------------------------------------
# discover (integration-level)
# ---------------------------------------------------------------------------

class TestDiscoverIntegration(unittest.TestCase):

    def test_discover_adds_dynamic_entries_with_client(self):
        with patch("tap_qualtrics.discover._apply_access_checks"):
            with patch("tap_qualtrics.discover._add_dynamic_entries") as mock_add:
                discover(client=MagicMock())
        mock_add.assert_called_once()

    def test_discover_no_client_no_dynamic(self):
        with patch("tap_qualtrics.discover._add_dynamic_entries") as mock_add:
            catalog = discover(client=None)
        mock_add.assert_not_called()
        self.assertGreater(len(catalog.streams), 0)

    def test_discover_schema_from_dict_error_propagates(self):
        """Cover except block (lines 127-135) when Schema.from_dict raises."""
        with patch("tap_qualtrics.discover.get_schemas") as mock_schemas:
            mock_schemas.return_value = (
                {"bad_stream": {"type": "object"}},
                {"bad_stream": []},
            )
            with patch("tap_qualtrics.discover.Schema.from_dict", side_effect=ValueError("bad schema")):
                with self.assertRaises(ValueError):
                    discover(client=None)

    def test_discover_dynamic_stream_in_schemas_is_skipped(self):
        """Cover line 127: continue when stream_name is in _DYNAMIC_SCHEMA_STREAMS."""
        with patch("tap_qualtrics.discover.get_schemas") as mock_schemas:
            mock_schemas.return_value = (
                {
                    "audit_export": {"type": "object", "properties": {}},
                    "users": {"type": "object", "properties": {}},
                },
                {
                    "audit_export": [],
                    "users": [],
                },
            )
            catalog = discover(client=None)
        stream_ids = [s.tap_stream_id for s in catalog.streams]
        # audit_export is dynamic, should be skipped in static loop
        self.assertNotIn("audit_export", stream_ids)
        # users is a real stream, should be present
        self.assertIn("users", stream_ids)

    def test_apply_access_checks_stream_not_in_streams_dict(self):
        """Cover line 71: continue when stream_cls is None (name in schemas but not STREAMS)."""
        schemas = {"ghost_stream": {"type": "object"}, "users": {"type": "object"}}
        fm = {"ghost_stream": [], "users": []}
        users_cls = MagicMock()
        users_inst = MagicMock()
        users_inst.check_access.return_value = True
        users_inst._make_probe_path.return_value = ""
        users_inst._enrich_sample.side_effect = lambda s, p: s
        users_cls.return_value = users_inst
        users_cls.parent = None
        # ghost_stream is NOT in STREAMS → STREAMS.get("ghost_stream") → None → continue (line 71)
        streams = {"users": users_cls}  # ghost_stream absent
        with patch("tap_qualtrics.discover.STREAMS", streams):
            _apply_access_checks(MagicMock(), schemas, fm)
        # users should still be accessible
        self.assertIn("users", schemas)


if __name__ == "__main__":
    unittest.main()
