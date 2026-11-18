import os

from tap_tester import connections

from base import QualtricsBaseTest


class QualtricsAuthConfigNegativeIntegrationTest(QualtricsBaseTest):
    """Integration-negative coverage for auth, config keys and start_date behavior."""

    @staticmethod
    def name():
        return "tap_tester_qualtrics_auth_config_negative_test"

    def tearDown(self):
        super().tearDown()
        self.clear_test_config_overrides()

    def streams_to_test(self):
        # Keep stable and small; include at least one incremental stream for start_date checks.
        return {"users", "surveys"}

    def _run_check_expect_failure(self, credential_overrides=None, property_overrides=None):
        self.set_test_config_overrides(
            credential_overrides=credential_overrides,
            property_overrides=property_overrides,
        )
        with self.assertRaises(Exception) as context:
            conn_id = connections.ensure_connection(self, original_properties=False)
            self.run_and_verify_check_mode(conn_id)
        return str(context.exception)

    def _run_sync_expect_failure(self, credential_overrides=None, property_overrides=None):
        self.set_test_config_overrides(
            credential_overrides=credential_overrides,
            property_overrides=property_overrides,
        )
        conn_id = connections.ensure_connection(self, original_properties=False)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        test_catalogs = [
            catalog for catalog in found_catalogs
            if catalog.get("stream_name") in self.streams_to_test()
        ]
        self.select_all_streams_and_fields(conn_id, test_catalogs)

        with self.assertRaises(Exception) as context:
            self.run_and_verify_sync_mode(conn_id)
        return str(context.exception)

    def test_client_credentials_bad_credentials(self):
        message = self._run_check_expect_failure(
            credential_overrides={"auth_method": "client_credentials", "client_secret": "wrong-secret"}
        )
        self.assertIn("Failed to obtain OAuth2 token", message)

    def test_wrong_data_center(self):
        message = self._run_check_expect_failure(
            credential_overrides={"data_center": "invalid-data-center-for-negative-test"}
        )
        self.assertTrue(
            "Failed to obtain OAuth2 token" in message
            or "Invalid data_center" in message
            or "Name or service not known" in message
            or "host" in message.lower()
        )

    def test_authorization_code_missing_refresh_and_access_token(self):
        message = self._run_check_expect_failure(
            credential_overrides={
                "auth_method": "authorization_code",
                "access_token": None,
                "refresh_token": None,
            }
        )
        self.assertIn("refresh_token", message)

    def test_missing_required_config_key_client_secret(self):
        message = self._run_check_expect_failure(
            credential_overrides={"client_secret": None}
        )
        self.assertIn("client_secret", message)

    def test_missing_required_config_key_start_date(self):
        message = self._run_check_expect_failure(
            property_overrides={"start_date": None}
        )
        self.assertIn("start_date", message)

    def test_invalid_start_date_fails_during_sync(self):
        message = self._run_sync_expect_failure(
            property_overrides={"start_date": "not-a-rfc3339-date"}
        )
        self.assertIn("Invalid bookmark datetime value", message)

    def test_extra_config_keys_are_tolerated(self):
        self.set_test_config_overrides(
            credential_overrides={"unexpected_auth_key": "ok"},
            property_overrides={"unexpected_property_key": "ok"},
        )
        conn_id = connections.ensure_connection(self, original_properties=False)
        found_catalogs = self.run_and_verify_check_mode(conn_id)
        self.assertGreater(len(found_catalogs), 0)

    def test_authorization_code_missing_scope_negative(self):
        limited_scope_access_token = os.getenv("TAP_QUALTRICS_LIMITED_SCOPE_ACCESS_TOKEN")
        if not limited_scope_access_token:
            self.skipTest(
                "Set TAP_QUALTRICS_LIMITED_SCOPE_ACCESS_TOKEN to run missing-scope authorization_code negative test."
            )

        message = self._run_check_expect_failure(
            credential_overrides={
                "auth_method": "authorization_code",
                "access_token": limited_scope_access_token,
            }
        )
        self.assertIn("required scopes", message.lower())
