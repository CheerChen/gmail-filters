from __future__ import annotations

import math
import tempfile
import unittest
from datetime import date
from pathlib import Path

import audit_filters
import gmail_common


class FakeRequest:
    def __init__(self, response):
        self._response = response

    def execute(self):
        if callable(self._response):
            return self._response()
        return self._response


class FakeLabelsResource:
    def __init__(self, responses):
        self._responses = iter(responses)

    def list(self, **_kwargs):
        return FakeRequest(next(self._responses))


class FakeFiltersResource:
    def __init__(self, responses, delete_outcomes=None):
        self._responses = iter(responses)
        self._delete_outcomes = delete_outcomes or {}
        self.deleted_ids = []

    def list(self, **_kwargs):
        return FakeRequest(next(self._responses))

    def delete(self, **kwargs):
        filter_id = kwargs["id"]

        def execute_delete():
            outcome = self._delete_outcomes.get(filter_id, {})
            if isinstance(outcome, Exception):
                raise outcome
            self.deleted_ids.append(filter_id)
            return outcome

        return FakeRequest(execute_delete)


class FakeMessagesResource:
    def __init__(self, list_responses, get_responses):
        self._list_responses = list_responses
        self._get_responses = get_responses

    def list(self, **kwargs):
        query = kwargs["q"]
        page_token = kwargs.get("pageToken")
        return FakeRequest(self._list_responses[(query, page_token)])

    def get(self, **kwargs):
        message_id = kwargs["id"]
        return FakeRequest(self._get_responses[message_id])


class FakeSettingsResource:
    def __init__(self, filters_resource):
        self._filters_resource = filters_resource

    def filters(self):
        return self._filters_resource


class FakeUsersResource:
    def __init__(self, labels_resource, filters_resource, messages_resource):
        self._labels_resource = labels_resource
        self._filters_resource = filters_resource
        self._messages_resource = messages_resource

    def labels(self):
        return self._labels_resource

    def settings(self):
        return FakeSettingsResource(self._filters_resource)

    def messages(self):
        return self._messages_resource


class StrictLabelsResource:
    def list(self, **kwargs):
        if kwargs != {"userId": "me"}:
            raise AssertionError(f"Unexpected kwargs for labels.list: {kwargs}")
        return FakeRequest({"labels": [{"id": "L1", "name": "Inbox/Sub"}]})


class StrictFiltersResource:
    def list(self, **kwargs):
        if kwargs != {"userId": "me"}:
            raise AssertionError(f"Unexpected kwargs for filters.list: {kwargs}")
        return FakeRequest({"filter": [{"id": "f1"}]})


class StrictSettingsResource:
    def filters(self):
        return StrictFiltersResource()


class StrictUsersResource:
    def labels(self):
        return StrictLabelsResource()

    def settings(self):
        return StrictSettingsResource()


class StrictService:
    def users(self):
        return StrictUsersResource()


class FakeService:
    def __init__(
        self,
        labels_responses,
        filters_responses,
        list_responses,
        get_responses,
        delete_outcomes=None,
    ):
        self.filters_resource = FakeFiltersResource(
            filters_responses, delete_outcomes=delete_outcomes
        )
        self._users = FakeUsersResource(
            FakeLabelsResource(labels_responses),
            self.filters_resource,
            FakeMessagesResource(list_responses, get_responses),
        )

    def users(self):
        return self._users


class AuditFiltersTests(unittest.TestCase):
    def test_build_filter_query_combines_supported_fields(self):
        criteria = {
            "from": "alerts@example.com",
            "to": "team@example.com",
            "subject": "Build Failed",
            "query": "has:attachment newer_than:30d",
            "negatedQuery": "label:ignore",
        }

        query = audit_filters.build_filter_query(criteria)

        self.assertEqual(
            query,
            "from:(alerts@example.com) to:(team@example.com) "
            "subject:(Build Failed) has:attachment newer_than:30d -(label:ignore)",
        )

    def test_build_filter_query_skips_empty_criteria(self):
        self.assertEqual(audit_filters.build_filter_query({}), "")
        self.assertEqual(audit_filters.build_filter_query(None), "")

    def test_parse_date_header_normalizes_timezone(self):
        parsed = gmail_common.parse_date_header("Tue, 09 Apr 2024 23:30:00 -0700")

        self.assertEqual(parsed.date().isoformat(), "2024-04-10")

    def test_token_looks_like_client_config_detects_oauth_client_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            token_path = Path(tmpdir) / "token.json"
            token_path.write_text('{"installed": {"client_id": "abc"}}', encoding="utf-8")

            self.assertTrue(gmail_common.token_looks_like_client_config(token_path))

    def test_select_results_applies_thresholds_unless_show_all(self):
        results = [
            audit_filters.FilterAuditResult(
                filter_id="1",
                query="old",
                labels=(),
                match_count=12,
                last_seen=date(2023, 1, 1),
                days_ago=400,
            ),
            audit_filters.FilterAuditResult(
                filter_id="2",
                query="fresh",
                labels=(),
                match_count=2,
                last_seen=date(2025, 1, 1),
                days_ago=20,
            ),
            audit_filters.FilterAuditResult(
                filter_id="3",
                query="never",
                labels=(),
                match_count=0,
                last_seen=None,
                days_ago=math.inf,
            ),
            audit_filters.FilterAuditResult(
                filter_id="4",
                query="low-volume-fresh",
                labels=(),
                match_count=5,
                last_seen=date(2025, 1, 10),
                days_ago=10,
            ),
        ]

        filtered = audit_filters.select_results(
            results,
            threshold_days=180,
            max_matches=None,
            show_all=False,
        )

        self.assertEqual([result.query for result in filtered], ["old", "never"])
        self.assertEqual(
            [
                result.query
                for result in audit_filters.select_results(
                    results,
                    threshold_days=180,
                    max_matches=None,
                    show_all=True,
                )
            ],
            ["old", "fresh", "never", "low-volume-fresh"],
        )
        self.assertEqual(
            [
                result.query
                for result in audit_filters.select_results(
                    results,
                    threshold_days=None,
                    max_matches=100,
                    show_all=False,
                )
            ],
            ["old", "fresh", "never", "low-volume-fresh"],
        )
        self.assertEqual(
            [
                result.query
                for result in audit_filters.select_results(
                    results,
                    threshold_days=180,
                    max_matches=100,
                    show_all=False,
                )
            ],
            ["old", "never"],
        )

    def test_fetch_labels_and_filters_use_supported_kwargs_only(self):
        service = StrictService()

        labels = gmail_common.fetch_label_map(service, sleep_seconds=0, sleep_fn=lambda _: None)
        filters = gmail_common.fetch_filters(service, sleep_seconds=0, sleep_fn=lambda _: None)

        self.assertEqual(labels, {"L1": "Inbox/Sub"})
        self.assertEqual(filters, [{"id": "f1"}])

    def test_parse_args_supports_cleanup_flag_and_matches(self):
        args = audit_filters.parse_args(["--days", "30", "--matches", "100", "--cleanup"])

        self.assertEqual(args.days, 30)
        self.assertEqual(args.matches, 100)
        self.assertTrue(args.cleanup)
        self.assertFalse(args.show_all)

    def test_resolve_selection_criteria_preserves_default_and_matches_only_modes(self):
        self.assertEqual(
            audit_filters.resolve_selection_criteria(
                threshold_days=None,
                max_matches=None,
                show_all=False,
            ),
            (180, None),
        )
        self.assertEqual(
            audit_filters.resolve_selection_criteria(
                threshold_days=None,
                max_matches=100,
                show_all=False,
            ),
            (None, 100),
        )
        self.assertEqual(
            audit_filters.resolve_selection_criteria(
                threshold_days=30,
                max_matches=100,
                show_all=False,
            ),
            (30, 100),
        )
        self.assertEqual(
            audit_filters.resolve_selection_criteria(
                threshold_days=30,
                max_matches=100,
                show_all=True,
            ),
            (None, None),
        )

    def test_audit_filters_fetches_dates_and_sorts_stalest_first(self):
        service = FakeService(
            labels_responses=[
                {"labels": [{"id": "Label_1", "name": "Finance/OldBank"}]}
            ],
            filters_responses=[
                {
                    "filter": [
                        {
                            "id": "f1",
                            "criteria": {"from": "noreply@oldbank.com"},
                            "action": {"addLabelIds": ["Label_1"]},
                        },
                        {
                            "id": "f2",
                            "criteria": {"from": "notifications@github.com"},
                            "action": {"addLabelIds": ["Missing_Label"]},
                        },
                        {
                            "id": "f3",
                            "criteria": {},
                            "action": {"addLabelIds": ["Label_1"]},
                        },
                    ]
                }
            ],
            list_responses={
                ("from:(noreply@oldbank.com)", None): {
                    "messages": [{"id": "m1"}],
                    "nextPageToken": "page-2",
                },
                ("from:(noreply@oldbank.com)", "page-2"): {
                    "messages": [{"id": "m2"}, {"id": "m3"}],
                },
                ("from:(notifications@github.com)", None): {"messages": []},
            },
            get_responses={
                "m1": {
                    "payload": {
                        "headers": [
                            {
                                "name": "Date",
                                "value": "Tue, 01 Oct 2024 09:30:00 +0000",
                            }
                        ]
                    }
                }
            },
        )
        sleep_calls = []
        progress_updates = []

        results, summary = audit_filters.audit_filters(
            service,
            today=date(2025, 4, 1),
            sleep_seconds=0.1,
            sleep_fn=sleep_calls.append,
            progress_callback=progress_updates.append,
        )

        self.assertEqual([result.filter_id for result in results], ["f2", "f1"])
        self.assertTrue(math.isinf(results[0].days_ago))
        self.assertEqual(results[1].days_ago, 182)
        self.assertEqual(results[0].match_count, 0)
        self.assertEqual(results[1].match_count, 3)
        self.assertEqual(results[0].labels, ("Unknown(Missing_Label)",))
        self.assertEqual(summary["total_filters"], 3)
        self.assertEqual(summary["skipped_empty_criteria"], 1)
        self.assertEqual(summary["no_match"], 1)
        self.assertEqual(summary["over_180"], 2)
        self.assertEqual(summary["over_90"], 2)
        self.assertEqual(len(sleep_calls), 6)
        self.assertEqual(progress_updates[0].message, "Loading labels")
        self.assertEqual(progress_updates[1].message, "Loaded 1 labels")
        self.assertEqual(progress_updates[2].message, "Loading filters")
        self.assertEqual(progress_updates[3].message, "Scanning 3 filters")
        self.assertTrue(
            any(update.message == "Skipped empty criteria" for update in progress_updates)
        )
        self.assertEqual(progress_updates[-1].stage, "done")
        self.assertEqual(progress_updates[-1].completed, 3)
        self.assertEqual(progress_updates[-1].total, 3)
        self.assertEqual(progress_updates[-1].no_match, 1)
        self.assertEqual(progress_updates[-1].over_180, 2)
        self.assertEqual(progress_updates[-1].skipped_empty, 1)

    def test_fetch_latest_message_info_counts_exact_matches_for_one_label_query(self):
        service = FakeService(
            labels_responses=[{"labels": []}],
            filters_responses=[{"filter": []}],
            list_responses={
                ("from:(@github.com)", None): {
                    "messages": [{"id": "m1"}, {"id": "m2"}],
                    "nextPageToken": "page-2",
                },
                ("from:(@github.com)", "page-2"): {
                    "messages": [{"id": "m3"}],
                },
            },
            get_responses={
                "m1": {
                    "payload": {
                        "headers": [
                            {
                                "name": "Date",
                                "value": "Wed, 02 Apr 2026 08:00:00 +0000",
                            }
                        ]
                    }
                }
            },
        )

        last_seen, match_count = audit_filters.fetch_latest_message_info(
            service,
            "from:(@github.com)",
            sleep_seconds=0,
            sleep_fn=lambda _: None,
        )

        self.assertEqual(last_seen, date(2026, 4, 2))
        self.assertEqual(match_count, 3)

    def test_confirm_cleanup_accepts_yes_only(self):
        self.assertTrue(
            audit_filters.confirm_cleanup(2, input_fn=lambda _prompt: "y")
        )
        self.assertTrue(
            audit_filters.confirm_cleanup(2, input_fn=lambda _prompt: "YES")
        )
        self.assertFalse(
            audit_filters.confirm_cleanup(2, input_fn=lambda _prompt: "")
        )
        self.assertFalse(
            audit_filters.confirm_cleanup(2, input_fn=lambda _prompt: "n")
        )

    def test_delete_filters_deletes_each_filter_with_progress_output(self):
        service = FakeService(
            labels_responses=[{"labels": []}],
            filters_responses=[{"filter": []}],
            list_responses={},
            get_responses={},
        )
        printed = []
        sleep_calls = []
        results = [
            audit_filters.FilterAuditResult(
                filter_id="f1",
                query="from:(@github.com)",
                labels=("Github",),
                match_count=91,
                last_seen=date(2026, 4, 2),
                days_ago=7,
            ),
            audit_filters.FilterAuditResult(
                filter_id="f2",
                query="from:(amazon)",
                labels=("Amazon",),
                match_count=378,
                last_seen=date(2026, 4, 1),
                days_ago=8,
            ),
        ]

        summary = audit_filters.delete_filters(
            service,
            results,
            print_fn=printed.append,
            error_fn=printed.append,
            sleep_seconds=0.2,
            sleep_fn=sleep_calls.append,
        )

        self.assertEqual(service.filters_resource.deleted_ids, ["f1", "f2"])
        self.assertEqual(
            printed,
            [
                "Deleted 1/2: from:(@github.com) [Github]",
                "Deleted 2/2: from:(amazon) [Amazon]",
            ],
        )
        self.assertEqual(sleep_calls, [0.2, 0.2])
        self.assertEqual(summary, {"deleted": 2, "errors": 0, "total": 2})

    def test_delete_filters_continues_after_permission_error(self):
        service = FakeService(
            labels_responses=[{"labels": []}],
            filters_responses=[{"filter": []}],
            list_responses={},
            get_responses={},
            delete_outcomes={
                "f1": RuntimeError("403 insufficientPermissions"),
                "f2": {},
            },
        )
        printed = []
        errors = []
        results = [
            audit_filters.FilterAuditResult(
                filter_id="f1",
                query="from:(@github.com)",
                labels=("Github",),
                match_count=91,
                last_seen=date(2026, 4, 2),
                days_ago=7,
            ),
            audit_filters.FilterAuditResult(
                filter_id="f2",
                query="from:(amazon)",
                labels=("Amazon",),
                match_count=378,
                last_seen=date(2026, 4, 1),
                days_ago=8,
            ),
        ]

        summary = audit_filters.delete_filters(
            service,
            results,
            print_fn=printed.append,
            error_fn=errors.append,
            sleep_seconds=0,
            sleep_fn=lambda _: None,
        )

        self.assertEqual(service.filters_resource.deleted_ids, ["f2"])
        self.assertEqual(printed, ["Deleted 2/2: from:(amazon) [Amazon]"])
        self.assertEqual(len(errors), 1)
        self.assertIn("Failed 1/2: from:(@github.com) [Github]", errors[0])
        self.assertIn("check Gmail scope requirements for filters.delete", errors[0])
        self.assertEqual(summary, {"deleted": 1, "errors": 1, "total": 2})

    def test_render_table_uses_formatted_values(self):
        captured = {}

        def fake_tabulate(rows, headers, **kwargs):
            captured["rows"] = rows
            captured["headers"] = headers
            captured["kwargs"] = kwargs
            return "table-output"

        output = audit_filters.render_table(
            [
                audit_filters.FilterAuditResult(
                    filter_id="1",
                    query="from:(orders@example.com)",
                    labels=("Shopping/DeadShop",),
                    match_count=0,
                    last_seen=None,
                    days_ago=math.inf,
                )
            ],
            tabulate_fn=fake_tabulate,
        )

        self.assertEqual(output, "table-output")
        self.assertEqual(
            captured["rows"],
            [["(no match)", "∞", "0", "Shopping/DeadShop", "from:(orders@example.com)"]],
        )
        self.assertEqual(
            captured["headers"],
            ["Last seen", "Days ago", "Matches", "Labels", "Filter Query"],
        )
        self.assertEqual(captured["kwargs"]["tablefmt"], "simple")


if __name__ == "__main__":
    unittest.main()
