from __future__ import annotations

import unittest
from datetime import date

import discover_domains


class FakeRequest:
    def __init__(self, response):
        self._response = response

    def execute(self):
        return self._response


class FakeBatchRequest:
    def __init__(self):
        self._requests: list[tuple] = []

    def add(self, request, callback):
        self._requests.append((request, callback))

    def execute(self):
        for request, callback in self._requests:
            response = request.execute()
            callback("id", response, None)


class FakeMessagesResource:
    def __init__(self, list_responses, get_responses):
        self._list_responses = list_responses
        self._get_responses = get_responses

    def list(self, **kwargs):
        page_token = kwargs.get("pageToken")
        return FakeRequest(self._list_responses[page_token])

    def get(self, **kwargs):
        message_id = kwargs["id"]
        return FakeRequest(self._get_responses[message_id])


class FakeUsersResource:
    def __init__(self, messages_resource):
        self._messages_resource = messages_resource

    def messages(self):
        return self._messages_resource


class FakeService:
    def __init__(self, list_responses, get_responses):
        self._users = FakeUsersResource(FakeMessagesResource(list_responses, get_responses))

    def users(self):
        return self._users

    def new_batch_http_request(self):
        return FakeBatchRequest()


class DiscoverDomainsTests(unittest.TestCase):
    def test_parse_args_supports_days_min_and_limit(self):
        args = discover_domains.parse_args(["--days", "30", "--min", "5", "--limit", "300"])

        self.assertEqual(args.days, 30)
        self.assertEqual(args.minimum_count, 5)
        self.assertEqual(args.limit, 300)

    def test_normalize_sender_domain_uses_registered_domain(self):
        extractor = discover_domains.build_domain_extractor()

        self.assertEqual(
            discover_domains.normalize_sender_domain("shipment.amazon.co.jp", extractor),
            "amazon.co.jp",
        )
        self.assertEqual(
            discover_domains.normalize_sender_domain("mail.github.com", extractor),
            "github.com",
        )

    def test_fetch_recent_message_ids_respects_limit(self):
        service = FakeService(
            list_responses={
                None: {
                    "messages": [{"id": f"m{i}"} for i in range(1, 501)],
                    "nextPageToken": "page-2",
                },
                "page-2": {
                    "messages": [{"id": f"m{i}"} for i in range(501, 1001)],
                    "nextPageToken": "page-3",
                },
            },
            get_responses={},
        )
        sleep_calls = []
        progress_updates = []

        message_ids, limit_hit = discover_domains.fetch_recent_message_ids(
            service,
            days=90,
            limit=600,
            sleep_seconds=0.1,
            sleep_fn=sleep_calls.append,
            progress_callback=progress_updates.append,
        )

        self.assertEqual(len(message_ids), 600)
        self.assertEqual(message_ids[0], "m1")
        self.assertEqual(message_ids[-1], "m600")
        self.assertTrue(limit_hit)
        self.assertEqual(sleep_calls, [0.1, 0.1])
        self.assertEqual(progress_updates[0].message, "Listing recent inbox messages")
        self.assertEqual(progress_updates[-1].completed, 600)
        self.assertTrue(progress_updates[-1].limit_hit)

    def test_discover_domains_aggregates_counts_and_samples(self):
        service = FakeService(
            list_responses={
                None: {
                    "messages": [
                        {"id": "m1"},
                        {"id": "m2"},
                        {"id": "m3"},
                        {"id": "m4"},
                    ]
                }
            },
            get_responses={
                "m1": {
                    "payload": {
                        "headers": [
                            {"name": "From", "value": "GitHub <noreply@mail.github.com>"},
                            {"name": "Date", "value": "Wed, 02 Apr 2026 08:00:00 +0000"},
                        ]
                    }
                },
                "m2": {
                    "payload": {
                        "headers": [
                            {"name": "From", "value": "GitHub <reply@notifications.github.com>"},
                            {"name": "Date", "value": "Thu, 03 Apr 2026 08:00:00 +0000"},
                        ]
                    }
                },
                "m3": {
                    "payload": {
                        "headers": [
                            {"name": "From", "value": "Amazon <orders@shipment.amazon.co.jp>"},
                            {"name": "Date", "value": "Tue, 01 Apr 2026 08:00:00 +0000"},
                        ]
                    }
                },
                "m4": {
                    "payload": {
                        "headers": [
                            {"name": "From", "value": "Broken Sender"},
                            {"name": "Date", "value": "Tue, 01 Apr 2026 08:00:00 +0000"},
                        ]
                    }
                },
            },
        )
        sleep_calls = []
        progress_updates = []

        candidates, summary = discover_domains.discover_domains(
            service,
            days=90,
            minimum_count=1,
            limit=2000,
            list_sleep_seconds=0.1,
            detail_sleep_seconds=0.05,
            sleep_fn=sleep_calls.append,
            extractor=discover_domains.build_domain_extractor(),
            progress_callback=progress_updates.append,
        )

        self.assertEqual([candidate.domain for candidate in candidates], ["github.com", "amazon.co.jp"])
        self.assertEqual(candidates[0].count, 2)
        self.assertEqual(candidates[0].last_seen, date(2026, 4, 3))
        self.assertEqual(candidates[0].sample_senders, ("noreply", "reply"))
        self.assertEqual(candidates[0].display_name, "GitHub")
        self.assertEqual(candidates[1].sample_senders, ("orders",))
        self.assertEqual(candidates[1].display_name, "Amazon")
        self.assertEqual(summary.total_message_ids, 4)
        self.assertEqual(summary.total_messages_inspected, 4)
        self.assertEqual(summary.unique_domains_found, 2)
        self.assertEqual(summary.displayed_domains, 2)
        self.assertFalse(summary.limit_hit)
        # 1 list call (0.1) + 1 batch call (0.05) = 2 sleeps
        self.assertEqual(sleep_calls, [0.1, 0.05])
        self.assertEqual(progress_updates[0].message, "Listing recent inbox messages")
        self.assertEqual(progress_updates[1].message, "Collected recent inbox message IDs")
        self.assertEqual(progress_updates[2].message, "Fetching sender metadata")
        self.assertEqual(progress_updates[-1].stage, "done")

    def test_discover_domains_applies_minimum_count(self):
        service = FakeService(
            list_responses={None: {"messages": [{"id": "m1"}, {"id": "m2"}]}},
            get_responses={
                "m1": {
                    "payload": {
                        "headers": [
                            {"name": "From", "value": "GitHub <noreply@mail.github.com>"},
                            {"name": "Date", "value": "Wed, 02 Apr 2026 08:00:00 +0000"},
                        ]
                    }
                },
                "m2": {
                    "payload": {
                        "headers": [
                            {"name": "From", "value": "Amazon <orders@shipment.amazon.co.jp>"},
                            {"name": "Date", "value": "Tue, 01 Apr 2026 08:00:00 +0000"},
                        ]
                    }
                },
            },
        )

        candidates, summary = discover_domains.discover_domains(
            service,
            days=90,
            minimum_count=2,
            limit=2000,
            list_sleep_seconds=0,
            detail_sleep_seconds=0,
            sleep_fn=lambda _: None,
            extractor=discover_domains.build_domain_extractor(),
        )

        self.assertEqual(candidates, [])
        self.assertEqual(summary.displayed_domains, 0)

    def test_render_table_formats_values(self):
        captured = {}

        def fake_tabulate(rows, headers, **kwargs):
            captured["rows"] = rows
            captured["headers"] = headers
            captured["kwargs"] = kwargs
            return "table-output"

        output = discover_domains.render_table(
            [
                discover_domains.DomainCandidate(
                    domain="github.com",
                    count=47,
                    last_seen=date(2026, 4, 8),
                    sample_senders=("noreply", "support", "reply"),
                    display_name="GitHub",
                )
            ],
            tabulate_fn=fake_tabulate,
        )

        self.assertEqual(output, "table-output")
        self.assertEqual(
            captured["rows"],
            [["47", "2026-04-08", "github.com", "GitHub", "noreply, support, reply"]],
        )
        self.assertEqual(
            captured["headers"],
            ["Count", "Last seen", "Domain", "Label name", "Sample senders"],
        )
        self.assertEqual(captured["kwargs"]["tablefmt"], "simple")


if __name__ == "__main__":
    unittest.main()
