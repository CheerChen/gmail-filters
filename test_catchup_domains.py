from __future__ import annotations

import unittest

import catchup_domains


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


class FakeExtractor:
    """Stub that returns domain as-is (no TLD extraction)."""
    def __call__(self, domain: str):
        return _FakeResult(domain)


class _FakeResult:
    def __init__(self, domain: str):
        self.top_domain_under_public_suffix = domain


# -- Fixtures --

FILTERS = [
    {"id": "f1", "criteria": {"from": "@shop.com"}, "action": {"addLabelIds": ["lbl_shop"]}},
    {"id": "f2", "criteria": {"from": "@news.org"}, "action": {"addLabelIds": ["lbl_news"]}},
]
LABELS = [
    {"id": "lbl_shop", "name": "Domains/Shop", "type": "user"},
    {"id": "lbl_news", "name": "Domains/News", "type": "user"},
    {"id": "INBOX", "name": "INBOX", "type": "system"},
]


def _make_message_meta(msg_id: str, from_header: str) -> dict:
    return {
        "id": msg_id,
        "payload": {"headers": [{"name": "From", "value": from_header}]},
    }


class FakeMessagesResource:
    def __init__(self, list_responses, get_responses):
        self._list_responses = list_responses
        self._get_responses = get_responses
        self._batch_modify_calls: list[dict] = []

    def list(self, **kwargs):
        pt = kwargs.get("pageToken")
        return FakeRequest(self._list_responses[pt])

    def get(self, **kwargs):
        return FakeRequest(self._get_responses[kwargs["id"]])

    def batchModify(self, **kwargs):
        self._batch_modify_calls.append(kwargs.get("body", {}))
        return FakeRequest({})


class FakeFiltersResource:
    def __init__(self, filters):
        self._filters = filters

    def list(self, **kwargs):
        return FakeRequest({"filter": self._filters})


class FakeLabelsResource:
    def __init__(self, labels):
        self._labels = labels

    def list(self, **kwargs):
        return FakeRequest({"labels": self._labels})


class FakeSettingsResource:
    def __init__(self, filters_resource):
        self._fr = filters_resource

    def filters(self):
        return self._fr


class FakeUsersResource:
    def __init__(self, messages, settings, labels):
        self._m = messages
        self._s = settings
        self._l = labels

    def messages(self):
        return self._m

    def settings(self):
        return self._s

    def labels(self):
        return self._l


class FakeService:
    def __init__(self, filters, labels, list_responses, get_responses):
        self._messages = FakeMessagesResource(list_responses, get_responses)
        self._users = FakeUsersResource(
            self._messages,
            FakeSettingsResource(FakeFiltersResource(filters)),
            FakeLabelsResource(labels),
        )

    def users(self):
        return self._users

    def new_batch_http_request(self):
        return FakeBatchRequest()


class TestExtractSenderDomain(unittest.TestCase):
    def test_extracts_domain(self):
        meta = _make_message_meta("m1", "Alice <alice@shop.com>")
        self.assertEqual(catchup_domains.extract_sender_domain(meta), "shop.com")

    def test_returns_none_for_missing_from(self):
        meta = {"id": "m1", "payload": {"headers": []}}
        self.assertIsNone(catchup_domains.extract_sender_domain(meta))


class TestBuildDomainFilterMap(unittest.TestCase):
    def test_builds_map(self):
        svc = FakeService(FILTERS, LABELS, {}, {})
        result = catchup_domains.build_domain_filter_map(svc, sleep_seconds=0)
        self.assertEqual(result["shop.com"], ("lbl_shop", "Domains/Shop"))
        self.assertEqual(result["news.org"], ("lbl_news", "Domains/News"))
        self.assertNotIn("INBOX", result)


class TestFindCatchupItems(unittest.TestCase):
    def _make_service(self, inbox_msgs: dict[str, str]):
        """inbox_msgs: {msg_id: from_header}"""
        list_resp = {None: {"messages": [{"id": mid} for mid in inbox_msgs]}}
        get_resp = {mid: _make_message_meta(mid, fh) for mid, fh in inbox_msgs.items()}
        return FakeService(FILTERS, LABELS, list_resp, get_resp)

    def test_catches_missed_messages(self):
        svc = self._make_service({
            "m1": "Shop <order@shop.com>",
            "m2": "News <daily@news.org>",
            "m3": "Friend <bob@personal.com>",
        })
        items = catchup_domains.find_catchup_items(
            svc, days=90, limit=5000, sleep_seconds=0, extractor=FakeExtractor()
        )
        domains = {item.domain for item in items}
        self.assertEqual(domains, {"shop.com", "news.org"})
        # personal.com has no filter, should not appear
        self.assertNotIn("personal.com", domains)

    def test_empty_inbox(self):
        svc = self._make_service({})
        items = catchup_domains.find_catchup_items(
            svc, days=90, limit=5000, sleep_seconds=0, extractor=FakeExtractor()
        )
        self.assertEqual(items, [])

    def test_no_matches(self):
        svc = self._make_service({
            "m1": "Random <x@unknown.com>",
        })
        items = catchup_domains.find_catchup_items(
            svc, days=90, limit=5000, sleep_seconds=0, extractor=FakeExtractor()
        )
        self.assertEqual(items, [])

    def test_sorted_by_count_desc(self):
        svc = self._make_service({
            "m1": "a@news.org",
            "m2": "b@shop.com",
            "m3": "c@shop.com",
        })
        items = catchup_domains.find_catchup_items(
            svc, days=90, limit=5000, sleep_seconds=0, extractor=FakeExtractor()
        )
        self.assertEqual(items[0].domain, "shop.com")
        self.assertEqual(len(items[0].message_ids), 2)
        self.assertEqual(items[1].domain, "news.org")


class TestRenderReport(unittest.TestCase):
    def test_empty(self):
        result = catchup_domains.render_report([])
        self.assertIn("No missed messages", result)

    def test_with_items(self):
        items = [
            catchup_domains.CatchupItem("shop.com", "Domains/Shop", ("m1", "m2")),
            catchup_domains.CatchupItem("news.org", "Domains/News", ("m3",)),
        ]
        result = catchup_domains.render_report(items)
        self.assertIn("shop.com", result)
        self.assertIn("Domains/Shop", result)
        self.assertIn("Total missed messages: 3", result)


class TestParseArgs(unittest.TestCase):
    def test_defaults(self):
        args = catchup_domains.parse_args([])
        self.assertEqual(args.days, 90)
        self.assertEqual(args.limit, 5000)

    def test_custom(self):
        args = catchup_domains.parse_args(["--days", "30", "--limit", "1000"])
        self.assertEqual(args.days, 30)
        self.assertEqual(args.limit, 1000)

    def test_apply_flag(self):
        args = catchup_domains.parse_args(["--apply"])
        self.assertTrue(args.apply)

    def test_all_flag(self):
        args = catchup_domains.parse_args(["--all"])
        self.assertTrue(getattr(args, "all"))


class TestApplyCatchup(unittest.TestCase):
    def test_applies_labels_and_archives(self):
        svc = FakeService(FILTERS, LABELS, {None: {"messages": []}}, {})
        domain_map = {
            "shop.com": ("lbl_shop", "Domains/Shop"),
            "news.org": ("lbl_news", "Domains/News"),
        }
        items = [
            catchup_domains.CatchupItem("shop.com", "Domains/Shop", ("m1", "m2")),
            catchup_domains.CatchupItem("news.org", "Domains/News", ("m3",)),
        ]
        catchup_domains.apply_catchup(svc, items, domain_map, sleep_seconds=0)

        calls = svc._messages._batch_modify_calls
        self.assertEqual(len(calls), 2)

        # First call: shop.com
        self.assertEqual(calls[0]["ids"], ["m1", "m2"])
        self.assertEqual(calls[0]["addLabelIds"], ["lbl_shop"])
        self.assertEqual(calls[0]["removeLabelIds"], ["INBOX"])

        # Second call: news.org
        self.assertEqual(calls[1]["ids"], ["m3"])
        self.assertEqual(calls[1]["addLabelIds"], ["lbl_news"])
        self.assertEqual(calls[1]["removeLabelIds"], ["INBOX"])


if __name__ == "__main__":
    unittest.main()
