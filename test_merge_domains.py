from __future__ import annotations

import unittest

import merge_domains


class FakeRequest:
    def __init__(self, response):
        self._response = response

    def execute(self):
        return self._response


class FakeMessagesResource:
    def __init__(self, list_responses=None):
        self._list_responses = list_responses or {None: {"messages": []}}
        self._batch_modify_calls: list[dict] = []

    def list(self, **kwargs):
        page_token = kwargs.get("pageToken")
        return FakeRequest(self._list_responses[page_token])

    def batchModify(self, **kwargs):
        self._batch_modify_calls.append(kwargs.get("body", {}))
        return FakeRequest({})


class FakeFiltersResource:
    def __init__(self, filters: list[dict]):
        self._filters = list(filters)
        self._deleted: list[str] = []
        self._created: list[dict] = []

    def list(self, **kwargs):
        return FakeRequest({"filter": self._filters})

    def delete(self, **kwargs):
        fid = kwargs["id"]
        self._deleted.append(fid)
        self._filters = [f for f in self._filters if f["id"] != fid]
        return FakeRequest({})

    def create(self, **kwargs):
        body = kwargs["body"]
        new_filter = {"id": "new_filter_id", **body}
        self._created.append(body)
        self._filters.append(new_filter)
        return FakeRequest(new_filter)


class FakeLabelsResource:
    def __init__(self, labels: list[dict]):
        self._labels = list(labels)
        self._deleted: list[str] = []

    def list(self, **kwargs):
        return FakeRequest({"labels": self._labels})

    def delete(self, **kwargs):
        lid = kwargs["id"]
        self._deleted.append(lid)
        return FakeRequest({})


class FakeSettingsResource:
    def __init__(self, filters_resource: FakeFiltersResource):
        self._filters = filters_resource

    def filters(self):
        return self._filters


class FakeUsersResource:
    def __init__(self, messages, settings, labels):
        self._messages = messages
        self._settings = settings
        self._labels = labels

    def messages(self):
        return self._messages

    def settings(self):
        return self._settings

    def labels(self):
        return self._labels


class FakeService:
    def __init__(self, filters, labels, messages_list_responses=None):
        self._messages = FakeMessagesResource(messages_list_responses)
        self._filters = FakeFiltersResource(filters)
        self._labels = FakeLabelsResource(labels)
        self._settings = FakeSettingsResource(self._filters)
        self._users = FakeUsersResource(self._messages, self._settings, self._labels)

    def users(self):
        return self._users


# ---------- Fixtures ----------

SRC_FILTER = {
    "id": "src_f1",
    "criteria": {"from": "@src.com"},
    "action": {"addLabelIds": ["lbl_src"], "removeLabelIds": ["INBOX"]},
}
TGT_FILTER = {
    "id": "tgt_f1",
    "criteria": {"from": "@tgt.com"},
    "action": {"addLabelIds": ["lbl_tgt"], "removeLabelIds": ["INBOX"]},
}
LABELS = [
    {"id": "lbl_src", "name": "Domains/Source Brand", "type": "user"},
    {"id": "lbl_tgt", "name": "Domains/Target Brand", "type": "user"},
]


class TestFindFilterForDomain(unittest.TestCase):
    def test_finds_matching_filter(self):
        result = merge_domains.find_filter_for_domain([SRC_FILTER, TGT_FILTER], "src.com")
        self.assertEqual(result["id"], "src_f1")

    def test_case_insensitive(self):
        result = merge_domains.find_filter_for_domain([SRC_FILTER], "SRC.COM")
        self.assertIsNotNone(result)

    def test_returns_none_when_not_found(self):
        result = merge_domains.find_filter_for_domain([SRC_FILTER], "nope.com")
        self.assertIsNone(result)


class TestValidateFilterShape(unittest.TestCase):
    def test_valid_filter_passes(self):
        # Should not raise
        merge_domains._validate_filter_shape(SRC_FILTER, "src.com")

    def test_invalid_filter_exits(self):
        bad_filter = {
            "criteria": {"from": "@bad.com"},
            "action": {"addLabelIds": ["lbl1"], "forward": "x@y.com"},
        }
        with self.assertRaises(SystemExit):
            merge_domains._validate_filter_shape(bad_filter, "bad.com")

    def test_missing_remove_labels_exits(self):
        bad_filter = {
            "criteria": {"from": "@bad.com"},
            "action": {"addLabelIds": ["lbl1"]},
        }
        with self.assertRaises(SystemExit):
            merge_domains._validate_filter_shape(bad_filter, "bad.com")


class TestReplacementFilterExists(unittest.TestCase):
    def test_true_when_exists(self):
        existing = {
            "id": "x",
            "criteria": {"from": "@src.com"},
            "action": {"addLabelIds": ["lbl_tgt"]},
        }
        self.assertTrue(
            merge_domains.replacement_filter_exists([existing], "src.com", "lbl_tgt")
        )

    def test_false_when_different_label(self):
        self.assertFalse(
            merge_domains.replacement_filter_exists([SRC_FILTER], "src.com", "lbl_tgt")
        )


class TestRunMerge(unittest.TestCase):
    def _make_service(self, msg_ids=None):
        msgs = msg_ids or []
        list_resp = {None: {"messages": [{"id": mid} for mid in msgs]}}
        return FakeService([SRC_FILTER, TGT_FILTER], LABELS, list_resp)

    def test_full_merge_flow(self):
        svc = self._make_service(["m1", "m2", "m3"])
        merge_domains.run_merge(
            svc,
            source_domain="src.com",
            target_domain="tgt.com",
            sleep_seconds=0,
            confirm_fn=lambda: True,
        )

        # Messages migrated
        self.assertEqual(len(svc._messages._batch_modify_calls), 1)
        call = svc._messages._batch_modify_calls[0]
        self.assertEqual(call["ids"], ["m1", "m2", "m3"])
        self.assertEqual(call["addLabelIds"], ["lbl_tgt"])
        self.assertEqual(call["removeLabelIds"], ["lbl_src"])

        # Source filter deleted
        self.assertIn("src_f1", svc._filters._deleted)

        # Replacement filter created
        self.assertEqual(len(svc._filters._created), 1)
        created = svc._filters._created[0]
        self.assertEqual(created["criteria"]["from"], "@src.com")
        self.assertEqual(created["action"]["addLabelIds"], ["lbl_tgt"])
        self.assertEqual(created["action"]["removeLabelIds"], ["INBOX"])

        # Source label deleted
        self.assertIn("lbl_src", svc._labels._deleted)

    def test_abort_does_nothing(self):
        svc = self._make_service(["m1"])
        merge_domains.run_merge(
            svc,
            source_domain="src.com",
            target_domain="tgt.com",
            sleep_seconds=0,
            confirm_fn=lambda: False,
        )
        self.assertEqual(svc._messages._batch_modify_calls, [])
        self.assertEqual(svc._filters._deleted, [])
        self.assertEqual(svc._filters._created, [])
        self.assertEqual(svc._labels._deleted, [])

    def test_no_messages(self):
        svc = self._make_service([])
        merge_domains.run_merge(
            svc,
            source_domain="src.com",
            target_domain="tgt.com",
            sleep_seconds=0,
            confirm_fn=lambda: True,
        )
        # No batchModify calls
        self.assertEqual(svc._messages._batch_modify_calls, [])
        # Filter and label still processed
        self.assertIn("src_f1", svc._filters._deleted)
        self.assertIn("lbl_src", svc._labels._deleted)

    def test_same_label_exits(self):
        same_label_filter = {
            "id": "f_same",
            "criteria": {"from": "@alias.com"},
            "action": {"addLabelIds": ["lbl_src"], "removeLabelIds": ["INBOX"]},
        }
        svc = FakeService(
            [SRC_FILTER, same_label_filter],
            LABELS,
            {None: {"messages": []}},
        )
        with self.assertRaises(SystemExit):
            merge_domains.run_merge(
                svc,
                source_domain="src.com",
                target_domain="alias.com",
                sleep_seconds=0,
                confirm_fn=lambda: True,
            )

    def test_skips_replacement_if_already_exists(self):
        # Pre-existing replacement filter
        existing_replacement = {
            "id": "existing_rep",
            "criteria": {"from": "@src.com"},
            "action": {"addLabelIds": ["lbl_tgt"], "removeLabelIds": ["INBOX"]},
        }
        svc = FakeService(
            [SRC_FILTER, TGT_FILTER, existing_replacement],
            LABELS,
            {None: {"messages": []}},
        )
        merge_domains.run_merge(
            svc,
            source_domain="src.com",
            target_domain="tgt.com",
            sleep_seconds=0,
            confirm_fn=lambda: True,
        )
        # Source filter deleted, but after re-fetch the replacement already exists
        # so no new filter created (the existing_replacement remains after src_f1 deletion)
        self.assertEqual(svc._filters._created, [])


class TestParseArgs(unittest.TestCase):
    def test_required_args(self):
        args = merge_domains.parse_args(["--source", "a.com", "--target", "b.com"])
        self.assertEqual(args.source, "a.com")
        self.assertEqual(args.target, "b.com")
        self.assertEqual(args.label_prefix, "Domains")

    def test_custom_prefix(self):
        args = merge_domains.parse_args(
            ["--source", "a.com", "--target", "b.com", "--label-prefix", "Mail"]
        )
        self.assertEqual(args.label_prefix, "Mail")


if __name__ == "__main__":
    unittest.main()
