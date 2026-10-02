import io
from datetime import date, timedelta

import pandas as pd

from pipeline.ingest import claims
from pipeline.sources import generate_claims

DAY = date(2026, 9, 30)


def _raw(plan_key="evergreen", day=DAY):
    data = generate_claims.to_csv(generate_claims.build_rows(plan_key, day))
    return pd.read_csv(io.BytesIO(data), dtype=str, keep_default_na=False)


def _month(plan_key="evergreen"):
    return pd.concat([_raw(plan_key, DAY - timedelta(days=d)) for d in range(60)], ignore_index=True)


def test_bad_rows_are_rejected_with_reasons():
    raw = _month()
    clean, rejects = claims.normalize(raw, "evergreen")
    assert set(rejects["reject_reason"]) == {"missing_claim_id;", "invalid_service_date;"}
    assert 0 < len(rejects) / len(raw) < 0.10          # a few % of rows, like a real feed
    assert clean["claim_id"].notna().all() and clean["service_from"].notna().all()


def test_messy_values_are_normalized():
    clean, _ = claims.normalize(_raw(), "evergreen")
    assert clean["member_id"].str.fullmatch(r"MEM\d{5}").all()                 # trimmed, upper
    rev = clean["revenue_code"].dropna()
    assert rev.str.fullmatch(r"\d{4}").all()                                    # "450" -> "0450"
    dx = clean["primary_dx"].dropna()
    assert dx[dx.str.len() > 3].str.contains(r"^[A-Z]\d\d\.", regex=True).all()  # "E119" -> "E11.9"
    assert set(clean["claim_status"]) <= {"paid", "adjusted", "void"}
    assert clean["health_plan"].eq("Evergreen Health Plan").all()


def test_money_parsing():
    s = pd.Series(["$1,234.50", "(12.50)", "99.10", None], dtype="string")
    assert claims._to_money(s).tolist()[:3] == [1234.50, -12.50, 99.10]


def test_icd10_dotting():
    assert claims._dot_icd10("E119") == "E11.9"
    assert claims._dot_icd10("t670xxa") == "T67.0XXA"
    assert claims._dot_icd10("I10") == "I10"
    assert claims._dot_icd10(None) is None


def test_mixed_date_formats_parse_to_same_day():
    s = pd.Series(["2026-09-15", "09/15/2026", "20260915"], dtype="string")
    assert set(claims._to_date(s)) == {date(2026, 9, 15)}


def test_duplicates_collapse_but_versions_survive():
    clean, _ = claims.normalize(_raw(), "evergreen")
    assert not claims.normalize(_month(), "evergreen")[0].duplicated().any()
    assert not clean.duplicated().any()
    # Restatements (7 replacement / 8 void) in a day's file always reference
    # claims first received exactly 7 days earlier.
    for d in range(60):
        day = DAY - timedelta(days=d)
        day_clean, _ = claims.normalize(_raw("evergreen", day), "evergreen")
        restated = day_clean[day_clean["freq_code"].isin(["7", "8"])]
        assert (restated["claim_id"].str[3:11] == (day - timedelta(days=7)).strftime("%Y%m%d")).all()


def test_restatement_rate_is_realistic():
    clean, _ = claims.normalize(_month(), "evergreen")
    share = clean["freq_code"].isin(["7", "8"]).mean()
    assert 0.02 < share < 0.20


def test_voids_carry_negative_paid_amounts():
    clean, _ = claims.normalize(pd.concat([_raw(day=DAY - timedelta(days=d)) for d in range(120)]), "evergreen")
    voids = clean[clean["claim_status"] == "void"]
    assert len(voids) > 0 and (voids["paid_amount"] < 0).all()


def test_frail_members_use_the_er_more():
    rows = [r for d in range(1, 120) for r in generate_claims.build_rows("harbor", date(2026, 5, 1).fromordinal(DAY.toordinal() - d))]
    df = pd.DataFrame(rows)
    df = df[df["CLAIM_ID"] != ""]
    df["member"] = df["MEMBER_ID"].str.strip().str.upper()
    df["frail"] = df["member"].map(generate_claims.frailty) > 0.5
    er_share = df.assign(er=df["POS"] == "23").groupby("frail")["er"].mean()
    assert er_share[True] > er_share[False]


def test_generator_is_deterministic():
    assert generate_claims.to_csv(generate_claims.build_rows("harbor", DAY)) == \
        generate_claims.to_csv(generate_claims.build_rows("harbor", DAY))
