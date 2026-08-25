"""Unit tests for discovery access-check logic."""
import unittest
from unittest.mock import MagicMock, patch

from tap_qualtrics.discover import _apply_access_checks, _prune_inaccessible_children, discover
from tap_qualtrics.exceptions import QualtricsForbiddenError


def _make_schemas_and_metadata(stream_names):
    """Return minimal schemas/field_metadata dicts for the given stream names."""
    schemas = {name: {"type": "object", "properties": {}} for name in stream_names}
    field_metadata = {name: [] for name in stream_names}
    return schemas, field_metadata


class TestPruneInaccessibleChildren(unittest.TestCase):
    """Tests for _prune_inaccessible_children."""

    def test_child_removed_when_parent_absent(self):
        schemas, fm = _make_schemas_and_metadata(["mailing_list_contacts"])
        # mailing_lists parent is not in schemas
        _prune_inaccessible_children(schemas, fm)
        self.assertNotIn("mailing_list_contacts", schemas)
        self.assertNotIn("mailing_list_contacts", fm)

    def test_child_kept_when_parent_present(self):
        # users has no parent; user is a child of users — both present, nothing pruned
        schemas, fm = _make_schemas_and_metadata(["users", "user"])
        _prune_inaccessible_children(schemas, fm)
        self.assertIn("user", schemas)

    def test_grandchild_removed_cascading(self):
        # surveys -> distributions -> distribution_history
        schemas, fm = _make_schemas_and_metadata(["distributions", "distribution_history"])
        # surveys is absent, so distributions should be removed, then distribution_history too
        _prune_inaccessible_children(schemas, fm)
        self.assertNotIn("distributions", schemas)
        self.assertNotIn("distribution_history", schemas)

    def test_parent_present_no_cascade(self):
        schemas, fm = _make_schemas_and_metadata(["surveys", "distributions", "distribution_history"])
        _prune_inaccessible_children(schemas, fm)
        self.assertIn("distributions", schemas)
        self.assertIn("distribution_history", schemas)

    def test_no_streams_affected_when_all_parents_present(self):
        schemas, fm = _make_schemas_and_metadata(["users", "user"])
        _prune_inaccessible_children(schemas, fm)
        self.assertIn("users", schemas)
        self.assertIn("user", schemas)


class TestApplyAccessChecks(unittest.TestCase):
    """Tests for _apply_access_checks."""

    def _make_mock_stream_cls(self, accessible=True, parent=None):
        inst = MagicMock()
        inst.check_access.return_value = accessible
        cls = MagicMock(return_value=inst)
        cls.parent = parent
        return cls

    def test_all_accessible_keeps_all_streams(self):
        schemas, fm = _make_schemas_and_metadata(["users", "groups"])
        streams = {
            "users": self._make_mock_stream_cls(accessible=True),
            "groups": self._make_mock_stream_cls(accessible=True),
        }
        with patch("tap_qualtrics.discover.STREAMS", streams):
            _apply_access_checks(MagicMock(), schemas, fm)
        self.assertIn("users", schemas)
        self.assertIn("groups", schemas)

    def test_inaccessible_stream_removed(self):
        schemas, fm = _make_schemas_and_metadata(["users", "groups"])
        streams = {
            "users": self._make_mock_stream_cls(accessible=False),
            "groups": self._make_mock_stream_cls(accessible=True),
        }
        with patch("tap_qualtrics.discover.STREAMS", streams):
            _apply_access_checks(MagicMock(), schemas, fm)
        self.assertNotIn("users", schemas)
        self.assertIn("groups", schemas)

    def test_raises_when_all_inaccessible(self):
        schemas, fm = _make_schemas_and_metadata(["users", "groups"])
        streams = {
            "users": self._make_mock_stream_cls(accessible=False),
            "groups": self._make_mock_stream_cls(accessible=False),
        }
        with patch("tap_qualtrics.discover.STREAMS", streams):
            with self.assertRaises(QualtricsForbiddenError):
                _apply_access_checks(MagicMock(), schemas, fm)

    def test_child_streams_cascade_removed_with_parent(self):
        # Include a second accessible parent so schemas is not empty after removal
        schemas, fm = _make_schemas_and_metadata(["users", "user", "groups"])
        user_child_cls = self._make_mock_stream_cls(accessible=True, parent="users")
        streams = {
            "users": self._make_mock_stream_cls(accessible=False),
            "user": user_child_cls,
            "groups": self._make_mock_stream_cls(accessible=True),
        }
        with patch("tap_qualtrics.discover.STREAMS", streams):
            _apply_access_checks(MagicMock(), schemas, fm)
        self.assertNotIn("users", schemas)
        self.assertNotIn("user", schemas)
        self.assertIn("groups", schemas)


class TestDiscover(unittest.TestCase):
    """Tests for the discover() function."""

    def test_discover_without_client_skips_access_checks(self):
        with patch("tap_qualtrics.discover._apply_access_checks") as mock_check:
            catalog = discover(client=None)
        mock_check.assert_not_called()
        self.assertIsNotNone(catalog)

    def test_discover_with_client_calls_access_checks(self):
        mock_client = MagicMock()
        with patch("tap_qualtrics.discover._apply_access_checks") as mock_check:
            discover(client=mock_client)
        mock_check.assert_called_once()

    def test_discover_returns_catalog_with_streams(self):
        mock_client = MagicMock()
        with patch("tap_qualtrics.discover._apply_access_checks"):
            catalog = discover(client=mock_client)
        self.assertTrue(len(catalog.streams) > 0)

    def test_discover_excludes_forbidden_streams(self):
        mock_client = MagicMock()

        def fake_access_checks(client, schemas, fm):
            schemas.pop("users", None)
            fm.pop("users", None)

        with patch("tap_qualtrics.discover._apply_access_checks", side_effect=fake_access_checks):
            catalog = discover(client=mock_client)

        stream_ids = [s.tap_stream_id for s in catalog.streams]
        self.assertNotIn("users", stream_ids)


class TestCheckAccess(unittest.TestCase):
    """Tests for BaseStream.check_access()."""

    def _make_stream(self, parent=None, path="test_path"):
        from tap_qualtrics.streams.abstracts import FullTableStream
        from typing import Any, Dict, Iterator
        from singer import Transformer

        class ConcreteStream(FullTableStream):
            tap_stream_id = "test"
            key_properties = ["id"]
            children = []

            def sync(self, state, transformer, parent_id=None):
                return 0

        ConcreteStream.parent = parent
        ConcreteStream.path = path
        return ConcreteStream

    def test_child_stream_always_accessible(self):
        StreamCls = self._make_stream(parent="some_parent")
        mock_client = MagicMock()
        mock_client.page_size = 100
        stream = StreamCls(client=mock_client)
        self.assertTrue(stream.check_access())
        mock_client.get.assert_not_called()

    def test_parent_stream_accessible(self):
        StreamCls = self._make_stream(parent=None, path="users")
        mock_client = MagicMock()
        mock_client.page_size = 100
        mock_client.get.return_value = {"result": {"elements": []}}
        stream = StreamCls(client=mock_client)
        self.assertTrue(stream.check_access())
        mock_client.get.assert_called_once_with("users", params={"pageSize": 1})

    def test_parent_stream_inaccessible_returns_false(self):
        StreamCls = self._make_stream(parent=None, path="users")
        mock_client = MagicMock()
        mock_client.page_size = 100
        mock_client.get.side_effect = QualtricsForbiddenError("403 Forbidden")
        stream = StreamCls(client=mock_client)
        self.assertFalse(stream.check_access())

    def test_parent_stream_not_found_returns_false(self):
        from tap_qualtrics.exceptions import QualtricsNotFoundError
        StreamCls = self._make_stream(parent=None, path="ticket-exports")
        mock_client = MagicMock()
        mock_client.page_size = 100
        mock_client.get.side_effect = QualtricsNotFoundError("404 Not Found")
        stream = StreamCls(client=mock_client)
        self.assertFalse(stream.check_access())


if __name__ == "__main__":
    unittest.main()
