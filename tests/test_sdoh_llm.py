import re
from datetime import datetime, timezone

import pytest

from fake_llm import FakeClient, parsed_response, refusal_response
from pipeline import config
from pipeline.ai import llm
from pipeline.enrich import sdoh_llm, sdoh_rules
from pipeline.schemas import MEMBER_SDOH_NEEDS, NOTE_CLASSIFICATIONS

NOW = datetime(2026, 10, 3, tzinfo=timezone.utc)
NOTES = [
    "Member mentioned food runs out before the end of the month. Member id MEM10042, call back (718) 555-0142.",
    "Member has no ride to his cardiology appointment next Tuesday.",
    "Member doing well, attended the walking group last week. No needs identified.",
    "Member asked us to stop calling. Please remove from call list.",
    "Left voicemail.",
]


def _rows(n):
    return [(f"00T{i:015d}", f"MEM{10000 + i}", NOTES[i % len(NOTES)], NOW) for i in range(n)]


def _rules_responder(kind, kw):
    """Fake model: tags each note in the prompt with the rule-based tagger."""
    notes = re.findall(r'<note id="([^"]+)">(.*?)</note>', kw["messages"][0]["content"], re.S)
    results = [sdoh_llm.NoteNeeds(note_id=i, needs=sdoh_rules.classify(t)) for i, t in notes]
    return parsed_response(sdoh_llm.NoteBatch(results=results))


@pytest.fixture
def fake(monkeypatch):
    def install(responder=_rules_responder, provider="anthropic"):
        monkeypatch.setattr(config, "LLM_PROVIDER", provider)
        client = FakeClient(responder)
        monkeypatch.setattr(llm, "client", lambda: client)
        return client
    return install


def test_categories_match_rules():
    assert set(sdoh_llm.Category.__args__) == set(sdoh_rules.RULES)


def test_rule_version_fits_column():
    assert len(sdoh_llm.rule_version()) <= 20 and sdoh_llm.rule_version().startswith(sdoh_llm.PROMPT_VERSION)


def test_switching_models_changes_rule_version(monkeypatch):
    # Different models must get different labels, so switching re-tags every note.
    monkeypatch.setattr(config, "LLM_MODEL", "us.anthropic.claude-haiku-4-5-20251001-v1:0")
    haiku = sdoh_llm.rule_version()
    monkeypatch.setattr(config, "LLM_MODEL", "us.anthropic.claude-opus-5-5")
    assert haiku == "p1:claude-haiku-4-5" and sdoh_llm.rule_version() == "p1:claude-opus-5-5"


def test_prompt_redacts_identifiers():
    prompt = sdoh_llm.build_prompt(_rows(1))
    assert "MEM10042" not in prompt and "555-0142" not in prompt
    assert "[MEMBER_ID]" in prompt and "[PHONE]" in prompt


def test_batches_of_twenty_and_every_note_tagged(fake):
    client = fake()
    usage = llm.Usage()
    tagged = sdoh_llm.classify(_rows(45), usage)
    assert len(client.messages.calls) == 3 and usage.requests == 3          # 20 + 20 + 5
    assert len(tagged) == 45
    assert tagged["00T000000000000001"] == ["transportation"]
    assert tagged["00T000000000000002"] == []                                 # "no needs identified"


def test_unknown_or_missing_ids_are_dropped(fake):
    def responder(kind, kw):
        return parsed_response(sdoh_llm.NoteBatch(results=[
            sdoh_llm.NoteNeeds(note_id="00T000000000000000", needs=["food_insecurity"]),
            sdoh_llm.NoteNeeds(note_id="NOT-SENT", needs=["transportation"]),
        ]))
    fake(responder)
    tagged = sdoh_llm.classify(_rows(3), llm.Usage())
    assert tagged == {"00T000000000000000": ["food_insecurity"]}               # others retried next run


def test_refused_batch_is_skipped(fake):
    fake(lambda kind, kw: refusal_response())
    usage = llm.Usage()
    assert sdoh_llm.classify(_rows(5), usage) == {} and usage.refusals == 1


def test_frames_use_llm_method():
    rows = _rows(4)
    tagged = {rows[0][0]: ["food_insecurity"], rows[3][0]: ["opt_out_request"], rows[2][0]: []}
    needs, classified = sdoh_llm.build_frames(rows, tagged, NOW)
    assert list(needs.columns) == MEMBER_SDOH_NEEDS.data_columns
    assert list(classified.columns) == NOTE_CLASSIFICATIONS.data_columns
    assert set(needs["method"]) == {"llm"} and len(needs) == 2
    assert len(classified) == 3                         # row 1 not tagged -> not marked as done


def test_main_skips_when_disabled():
    assert sdoh_llm.main("2026-10-03") == {"skipped": True}


def test_phi_guardrail_blocks_direct_api_for_real_data(fake, monkeypatch):
    client = fake()
    monkeypatch.setattr(config, "SYNTHETIC_DATA", False)
    assert sdoh_llm.main("2026-10-03")["reason"] == "phi_guardrail"
    assert client.messages.calls == []                  # nothing was sent


def test_main_degrades_when_llm_down(fake, monkeypatch):
    def boom(kind, kw):
        raise llm.LLMUnavailable("down")
    fake(boom)
    monkeypatch.setattr(sdoh_llm, "notes_to_classify", lambda limit: _rows(3))
    assert sdoh_llm.main("2026-10-03")["reason"] == "llm_unavailable"


def test_main_loads_through_the_standard_path(fake, monkeypatch):
    fake()
    calls = {}
    monkeypatch.setattr(sdoh_llm, "notes_to_classify", lambda limit: _rows(6))
    monkeypatch.setattr(sdoh_llm, "stage_and_merge", lambda *a, **k: calls.update(args=a, kwargs=k))
    summary = sdoh_llm.main("2026-10-03")
    assert summary["notes_tagged"] == 6 and summary["requests"] == 1
    assert calls["args"][3] == "core.sp_merge_note_classifications"
    assert calls["args"][0] == "sdoh_llm-2026-10-03"
