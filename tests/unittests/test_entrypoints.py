import runpy
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import tap_qualtrics


class TestInitEntrypoints(unittest.TestCase):

    @patch("tap_qualtrics.json.dump")
    @patch("tap_qualtrics.discover")
    def test_do_discover_emits_catalog(self, mock_discover, mock_dump):
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        tap_qualtrics.do_discover(client=MagicMock())

        mock_discover.assert_called_once()
        mock_dump.assert_called_once()

    @patch("tap_qualtrics.do_discover")
    @patch("tap_qualtrics.Client")
    @patch("singer.utils.parse_args")
    def test_main_discover_branch(self, mock_parse_args, mock_client_cls, mock_do_discover):
        mock_parse_args.return_value = SimpleNamespace(
            config={"x": 1},
            state={"bookmarks": {}},
            discover=True,
            catalog=None,
        )
        client = MagicMock()
        client_ctx = MagicMock()
        client_ctx.__enter__.return_value = client
        client_ctx.__exit__.return_value = False
        mock_client_cls.return_value = client_ctx

        tap_qualtrics.main()

        mock_do_discover.assert_called_once_with(client)

    @patch("tap_qualtrics.sync")
    @patch("tap_qualtrics.Client")
    @patch("singer.utils.parse_args")
    def test_main_catalog_branch_uses_empty_state_when_missing(
        self, mock_parse_args, mock_client_cls, mock_sync
    ):
        catalog = MagicMock()
        config = {"start_date": "2020-01-01T00:00:00Z"}
        mock_parse_args.return_value = SimpleNamespace(
            config=config,
            state=None,
            discover=False,
            catalog=catalog,
        )
        client = MagicMock()
        client_ctx = MagicMock()
        client_ctx.__enter__.return_value = client
        client_ctx.__exit__.return_value = False
        mock_client_cls.return_value = client_ctx

        tap_qualtrics.main()

        mock_sync.assert_called_once_with(
            client=client,
            config=config,
            catalog=catalog,
            state={},
        )

    @patch("json.dump")
    @patch("tap_qualtrics.discover.discover")
    @patch("tap_qualtrics.client.Client")
    @patch("singer.utils.parse_args")
    def test_module_main_invokes_main(
        self,
        mock_parse_args,
        mock_client_cls,
        mock_discover,
        _mock_json_dump,
    ):
        mock_parse_args.return_value = SimpleNamespace(
            config={"x": 1},
            state={},
            discover=True,
            catalog=None,
        )
        mock_catalog = MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        client = MagicMock()
        client_ctx = MagicMock()
        client_ctx.__enter__.return_value = client
        client_ctx.__exit__.return_value = False
        mock_client_cls.return_value = client_ctx

        runpy.run_path("tap_qualtrics/__init__.py", run_name="__main__")

        mock_parse_args.assert_called_once()


if __name__ == "__main__":
    unittest.main()
