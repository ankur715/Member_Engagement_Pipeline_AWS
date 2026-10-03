"""Tag social-needs (SDoH) categories in CHW notes with Claude -- a second
`method` ('llm') next to the rule-based tagger, stored in the same tables.

Why both: rules are transparent and cheap; an LLM catches phrasing the rules
miss ("the fridge is bare", "my son can't drive me anymore"). Keeping both,
per note, lets analytics.v_sdoh_method_agreement show where they disagree
before anyone trusts the LLM for decisions. The vulnerability index keeps
using the rule-based tags until that comparison is reviewed.

Safeguards:
  - Notes are PHI. Each note is run through phi.redact() before it's sent,
    and if SYNTHETIC_DATA is false, raw notes may only go to Bedrock (covered
    by the AWS BAA), never the direct Claude API.
  - Cost: at most LLM_MAX_NOTES_PER_RUN notes per run, 20 notes per request,
    low effort; only new / edited notes or a changed prompt version are sent.
  - The model can only answer with the fixed category list (structured output).
  - A refused or failed batch is skipped and retried on the next run.
"""
from datetime import date, datetime, timezone
from typing import Literal

from pydantic import BaseModel, Field

from pipeline import config, phi
from pipeline.ai import llm
from pipeline.enrich import sdoh_rules
from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.redshift import fetch_all
from pipeline.schemas import MEMBER_SDOH_NEEDS, NOTE_CLASSIFICATIONS

METHOD = "llm"
PROMPT_VERSION = "p1"      # bump when SYSTEM_PROMPT changes -> every note is re-tagged
BATCH_SIZE = 20

Category = Literal["food_insecurity", "transportation", "social_isolation",
                   "housing_instability", "medication_affordability", "opt_out_request"]
assert set(Category.__args__) == set(sdoh_rules.RULES), "LLM and rule categories must match"


class NoteNeeds(BaseModel):
    note_id: str = Field(description="The id attribute of the note, copied exactly")
    needs: list[Category] = Field(description="Needs this note shows for the member; empty if none")


class NoteBatch(BaseModel):
    results: list[NoteNeeds]


SYSTEM_PROMPT = """You tag notes written by community health workers (CHWs) after contacting \
health-plan members. For each note, list the social needs the note shows the member currently has.

Categories:
- food_insecurity: not enough food, skipping meals, running out of food, needs food benefits.
- transportation: no reliable way to get to appointments, pharmacy, or errands.
- social_isolation: lives alone without support, lonely, rarely leaves home or sees anyone.
- housing_instability: risk of eviction or losing housing, unaffordable rent, unsafe or \
unheated housing, landlord not making essential repairs.
- medication_affordability: cannot afford medications, skipping or rationing doses due to cost.
- opt_out_request: the member asked to stop being contacted.

Tag a need only when the note states or clearly implies it for the member now. A note saying \
there are no needs, or describing a need that is fully resolved, gets an empty list. Voicemails \
and unanswered calls get an empty list. Return exactly one result per note, using its id."""


def rule_version() -> str:
    # Stored in note_classifications.rule_version (VARCHAR 20): prompt + model.
    return f"{PROMPT_VERSION}:{config.LLM_MODEL}"[:20]


def notes_to_classify(limit: int) -> list[tuple]:
    """Newest notes not yet tagged by this method/prompt, or edited since."""
    return fetch_all(
        """
        SELECT e.activity_id, e.member_id, e.notes, e.last_modified_at
        FROM core.engagements e
        LEFT JOIN core.note_classifications c
               ON c.activity_id = e.activity_id AND c.method = %s
        WHERE e.notes IS NOT NULL
          AND (c.activity_id IS NULL
               OR c.note_modified_at < e.last_modified_at
               OR c.rule_version <> %s)
        ORDER BY e.last_modified_at DESC
        LIMIT %s;
        """,
        (METHOD, rule_version(), limit),
    )


def build_prompt(batch: list[tuple]) -> str:
    # Notes are redacted (member ids, phones, dates, emails) before leaving the warehouse.
    parts = [f'<note id="{activity_id}">{phi.redact(note)}</note>' for activity_id, _m, note, _t in batch]
    return "Tag these notes:\n" + "\n".join(parts)


def classify_batch(batch: list[tuple], usage: llm.Usage) -> dict[str, list[str]] | None:
    """{activity_id: [needs]} for one batch; None if the batch was refused."""
    result = llm.parse(SYSTEM_PROMPT, build_prompt(batch), NoteBatch, usage=usage, max_tokens=4000)
    if result.parsed is None:
        return None
    wanted = {row[0] for row in batch}
    # Keep only ids we actually sent; a note the model skipped stays unclassified (retried next run).
    return {r.note_id: sorted(set(r.needs)) for r in result.parsed.results if r.note_id in wanted}


def classify(rows: list[tuple], usage: llm.Usage) -> dict[str, list[str]]:
    tagged: dict[str, list[str]] = {}
    for i in range(0, len(rows), BATCH_SIZE):
        out = classify_batch(rows[i:i + BATCH_SIZE], usage)
        if out is None:
            continue                       # refused: leave these notes for the next run
        tagged.update(out)
    return tagged


def build_frames(rows: list[tuple], tagged: dict[str, list[str]], now: datetime):
    # Same shape as the rule-based tagger, with method='llm'.
    import pandas as pd
    needs, classified = [], []
    for activity_id, member_id, _note, modified_at in rows:
        if activity_id not in tagged:
            continue
        found = tagged[activity_id]
        needs += [{"activity_id": activity_id, "member_id": member_id, "need_category": c,
                   "method": METHOD, "detected_at": now} for c in found]
        classified.append({"activity_id": activity_id, "method": METHOD, "rule_version": rule_version(),
                           "needs_found": len(found), "note_modified_at": modified_at, "classified_at": now})
    return (pd.DataFrame(needs, columns=MEMBER_SDOH_NEEDS.data_columns),
            pd.DataFrame(classified, columns=NOTE_CLASSIFICATIONS.data_columns))


def main(batch_date: str | None = None) -> dict:
    batch_date = batch_date or date.today().isoformat()
    if not llm.enabled():
        print("LLM_PROVIDER is none -- skipping LLM note tagging.")
        return {"skipped": True}
    if config.LLM_PROVIDER == "anthropic" and not config.SYNTHETIC_DATA:
        # Real PHI only goes to a BAA-covered endpoint.
        print("SYNTHETIC_DATA=false: refusing to send notes to the direct Claude API; use LLM_PROVIDER=bedrock.")
        return {"skipped": True, "reason": "phi_guardrail"}

    rows = notes_to_classify(config.LLM_MAX_NOTES_PER_RUN)
    if not rows:
        print("No new or changed notes for the LLM tagger.")
        return {"notes": 0}
    usage = llm.Usage()
    try:
        tagged = classify(rows, usage)
    except llm.LLMUnavailable as exc:
        # Optional step: never fail the pipeline because the LLM is down.
        print(f"LLM unavailable, skipping this run: {exc}")
        return {"skipped": True, "reason": "llm_unavailable"}

    needs, classified = build_frames(rows, tagged, datetime.now(timezone.utc))
    if len(classified):
        stage_and_merge(load_id_for(f"sdoh_{METHOD}", batch_date), f"sdoh_{METHOD}",
                        [(MEMBER_SDOH_NEEDS, needs), (NOTE_CLASSIFICATIONS, classified)],
                        "core.sp_merge_note_classifications", source_uri=f"llm:{llm.model_id()}",
                        rows_in=len(rows), rows_rejected=len(rows) - len(classified))
    summary = {"notes_sent": len(rows), "notes_tagged": len(classified), "needs_found": len(needs),
               "requests": usage.requests, "input_tokens": usage.input_tokens,
               "output_tokens": usage.output_tokens, "refusals": usage.refusals}
    print(summary)   # counts only -- note text is PHI and is never printed
    return summary


if __name__ == "__main__":
    import sys
    main(sys.argv[1] if len(sys.argv) > 1 else date.today().isoformat())
