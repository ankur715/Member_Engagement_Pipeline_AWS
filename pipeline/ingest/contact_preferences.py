"""Do-not-contact list, maintained by hand by the member-services team in a
Google Sheet -> core.contact_preferences (full snapshot replace).

Hand-maintained sheets are messy: mixed date formats, free-text channel
values, stray whitespace, typo'd member ids, blank rows. Normalization is
conservative on purpose -- an unrecognised or blank channel is treated as
"all", because wrongly contacting someone who opted out is the expensive
mistake here. Unparseable rows are rejected to S3, never guessed at.
"""
import re
from datetime import date

import pandas as pd

from pipeline import config, http, s3_io
from pipeline.loaders import load_id_for, stage_and_merge
from pipeline.schemas import CONTACT_PREFERENCES

SOURCE = "contact_preferences"
MOCK_SHEET_ID = "mock-dnc-sheet"
RANGE = "DNC!A:E"                       # tab "DNC", columns A-E
MEMBER_ID = re.compile(r"^MEM\d{5}$")   # valid ids: MEM + exactly 5 digits
# Sheet header (lowercased) -> our column name.
HEADER = {"member id": "member_id", "date requested": "requested_date", "channel": "channel",
          "requested via": "requested_via", "notes": "notes"}


def read_sheet() -> list[list[str]]:
    # Real Google Sheet if credentials are configured...
    if config.GOOGLE_SERVICE_ACCOUNT_JSON and config.GOOGLE_DNC_SHEET_ID:
        import gspread
        gc = gspread.service_account(filename=config.GOOGLE_SERVICE_ACCOUNT_JSON)
        # get_all_values() -> list of rows, each a list of cell strings (row 0 = header).
        return gc.open_by_key(config.GOOGLE_DNC_SHEET_ID).worksheet("DNC").get_all_values()
    # ...otherwise the mock, which mirrors the Sheets API v4 values.get response shape.
    url = f"{config.MOCK_API_URL.rstrip('/')}/v4/spreadsheets/{MOCK_SHEET_ID}/values/{RANGE}"
    return http.get_json(http.session(), url).get("values", [])


def normalize_channel(value) -> str:
    # Free text typed by a person ("PHONE - calls only", "Mail only", "") -> phone / mail / all.
    v = str(value or "").strip().lower()
    has_phone, has_mail = "phone" in v or "call" in v, "mail" in v
    if has_phone and not has_mail:
        return "phone"
    if has_mail and not has_phone:
        return "mail"
    return "all"  # blank, "all", "everything", "phone + mail", or anything unrecognised


def normalize(values: list[list[str]]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (clean, rejects). Pure pandas -- unit tested without Google or AWS."""
    if not values:
        return pd.DataFrame(columns=CONTACT_PREFERENCES.data_columns), pd.DataFrame()
    # Map the sheet's header row to our column names (case/space-insensitive).
    header = [HEADER.get(h.strip().lower(), h.strip().lower()) for h in values[0]]
    width = len(header)
    rows = [(r + [""] * width)[:width] for r in values[1:]]  # Sheets API drops trailing empty cells
    raw = pd.DataFrame(rows, columns=header)
    for col in HEADER.values():  # tolerate a sheet missing optional columns
        if col not in raw.columns:
            raw[col] = ""
    raw = raw[raw.apply(lambda r: any(str(v).strip() for v in r), axis=1)]  # drop blank rows

    # Clean each column into the canonical shape.
    df = pd.DataFrame({
        "member_id": raw["member_id"].str.strip().str.upper().str.replace(r"\s+", "", regex=True),
        "channel": raw["channel"].map(normalize_channel),
        # format="mixed": each cell may use a different date style (9/3/2026, 2026-09-03, Sep 3, 2026).
        "requested_date": pd.to_datetime(raw["requested_date"].str.strip(), format="mixed", errors="coerce").dt.date,
        "requested_via": raw["requested_via"].str.strip().replace({"": pd.NA}),
    }, index=raw.index)

    # A typo'd id (e.g. letter O instead of zero) can't be matched to a member -> reject, don't guess.
    bad = ~df["member_id"].str.match(MEMBER_ID)
    rejects = raw.loc[bad].assign(reject_reason="invalid_member_id")
    # Duplicate entries: keep the EARLIEST request per member+channel
    # (the opt-out has been in force since then).
    clean = (df.loc[~bad]
             .sort_values("requested_date", na_position="last")
             .drop_duplicates(subset=["member_id", "channel"], keep="first"))
    return clean.reset_index(drop=True), rejects


def main(batch_date: str | None = None) -> dict:
    batch_date = batch_date or date.today().isoformat()
    values = read_sheet()                 # raw rows from the sheet
    clean, rejects = normalize(values)
    if len(rejects):                      # keep rejected rows in S3 for the ops team
        s3_io.put_bytes(f"rejects/{SOURCE}/dt={batch_date}/rejects.csv", rejects.to_csv(index=False).encode(), "text/csv")
    # sp_merge_contact_preferences refuses an empty snapshot, so an empty or
    # broken sheet fails loudly here instead of wiping everyone's opt-out.
    stage_and_merge(load_id_for(SOURCE, batch_date), SOURCE, [(CONTACT_PREFERENCES, clean)],
                    "core.sp_merge_contact_preferences", source_uri=f"sheets:{config.GOOGLE_DNC_SHEET_ID or MOCK_SHEET_ID}",
                    rows_in=max(len(values) - 1, 0), rows_rejected=len(rejects))
    summary = {"opt_outs": len(clean), "rejected": len(rejects)}
    print(summary)
    return summary


if __name__ == "__main__":
    main()
