# app.py
# Streamlit app: upload base + enrich spreadsheets, choose match columns, choose enrich columns,
# optionally dedupe enrich by latest timestamp, then download enriched file.

import re
from io import BytesIO
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Spreadsheet Enricher", layout="wide")


# ----------------------------
# Helpers
# ----------------------------
def normalize_text(x) -> str:
    if pd.isna(x):
        return ""
    s = str(x).strip().lower()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)  # remove punctuation
    return s


def is_blank(x) -> bool:
    return pd.isna(x) or (isinstance(x, str) and x.strip() == "")


def coerce_zip_like(x) -> str:
    # helpful for things like Zip Code; harmless for other strings
    if pd.isna(x):
        return ""
    s = str(x).strip()
    s = re.sub(r"\.0$", "", s)
    return s


def read_uploaded(file) -> pd.DataFrame:
    name = file.name.lower()
    if name.endswith((".xlsx", ".xls")):
        return pd.read_excel(file, dtype=str)
    if name.endswith(".csv"):
        return pd.read_csv(file, dtype=str)
    raise ValueError("Unsupported file type. Upload .csv, .xlsx, or .xls")


def make_key(df: pd.DataFrame, cols: list[str]) -> pd.Series:
    # build normalized join key across selected columns
    parts = []
    for c in cols:
        if c not in df.columns:
            parts.append(pd.Series([""] * len(df), index=df.index))
        else:
            parts.append(df[c].map(normalize_text))
    key = parts[0]
    for p in parts[1:]:
        key = key + "|" + p
    return key.str.strip("|")


def dedupe_enrich(enrich_df: pd.DataFrame, key_col: str, ts_col: str) -> pd.DataFrame:
    df = enrich_df.copy()
    df["_load_dt"] = pd.to_datetime(df[ts_col], errors="coerce", utc=False)
    df = df.sort_values(by=[key_col, "_load_dt"], ascending=[True, False])
    df = df.drop_duplicates(subset=[key_col], keep="first")
    df = df.drop(columns=["_load_dt"])
    return df


def enrich(
    base_df: pd.DataFrame,
    enrich_df: pd.DataFrame,
    match_cols: list[str],
    enrich_cols: list[str],
    overwrite: bool,
    dedupe: bool,
    ts_col: str | None,
) -> pd.DataFrame:
    base = base_df.copy()
    enr = enrich_df.copy()

    # light cleanup for zip-like fields
    for zc in ["Zip Code", "Zip", "zipcode", "zip_code"]:
        if zc in base.columns:
            base[zc] = base[zc].map(coerce_zip_like)
        if zc in enr.columns:
            enr[zc] = enr[zc].map(coerce_zip_like)

    base["_match_key"] = make_key(base, match_cols)
    enr["_match_key"] = make_key(enr, match_cols)

    if dedupe:
        if not ts_col or ts_col not in enr.columns:
            raise ValueError("Dedupe is enabled, but the selected timestamp column is missing.")
        enr = dedupe_enrich(enr, "_match_key", ts_col)

    # keep only needed columns in enrichment
    keep_cols = ["_match_key"] + [c for c in enrich_cols if c in enr.columns]
    enr_small = enr[keep_cols].copy()

    merged = base.merge(enr_small, on="_match_key", how="left", suffixes=("", "__enrich"))

    for c in enrich_cols:
        ec = f"{c}__enrich"
        if ec not in merged.columns:
            # column may not exist in enrich; skip
            continue

        if overwrite:
            merged[c] = merged[ec].where(~merged[ec].map(is_blank), merged.get(c, ""))
        else:
            if c not in merged.columns:
                merged[c] = ""
            base_blank = merged[c].map(is_blank)
            enrich_nonblank = ~merged[ec].map(is_blank)
            merged.loc[base_blank & enrich_nonblank, c] = merged.loc[base_blank & enrich_nonblank, ec]

        merged.drop(columns=[ec], inplace=True)

    merged.drop(columns=["_match_key"], inplace=True)
    return merged


def to_excel_bytes(df: pd.DataFrame) -> bytes:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="enriched")
    return buf.getvalue()


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


# ----------------------------
# UI
# ----------------------------
st.title("Spreadsheet Enricher")

with st.sidebar:
    st.header("1) Upload files")
    base_file = st.file_uploader("Base spreadsheet (.csv/.xlsx)", type=["csv", "xlsx", "xls"])
    enrich_file = st.file_uploader("Enrichment spreadsheet (.csv/.xlsx)", type=["csv", "xlsx", "xls"])

    st.divider()
    st.header("2) Options")
    overwrite = st.checkbox("Overwrite base values (otherwise fill blanks only)", value=False)
    output_format = st.radio("Output format", ["Excel (.xlsx)", "CSV (.csv)"], index=0)

    st.divider()
    st.header("3) Output file name")
    output_stem = st.text_input(
        "Filename (no extension)",
        value="enriched",
        help="We’ll add .xlsx or .csv automatically.",
    )
if not base_file or not enrich_file:
    st.info("Upload both a base file and an enrichment file to continue.")
    st.stop()

# Load data
try:
    base_df = read_uploaded(base_file)
    enrich_df = read_uploaded(enrich_file)
except Exception as e:
    st.error(f"Failed to read uploaded file(s): {e}")
    st.stop()

# Show previews
c1, c2 = st.columns(2)
with c1:
    st.subheader("Base preview")
    st.write(f"Rows: {len(base_df):,} | Cols: {len(base_df.columns):,}")
    st.dataframe(base_df.head(50), use_container_width=True)
with c2:
    st.subheader("Enrichment preview")
    st.write(f"Rows: {len(enrich_df):,} | Cols: {len(enrich_df.columns):,}")
    st.dataframe(enrich_df.head(50), use_container_width=True)

st.divider()
st.header("3) Matching + enrichment settings")

# Match columns selection (intersection is safest, but allow any base columns)
common_cols = sorted(set(base_df.columns).intersection(set(enrich_df.columns)))
if not common_cols:
    st.error("No overlapping column names between base and enrichment files. Matching requires shared columns.")
    st.stop()

match_cols = st.multiselect(
    "Select the column(s) to match on (used to link rows)",
    options=common_cols,
    default=[c for c in ["First", "Mid", "Last", "Suf"] if c in common_cols] or [common_cols[0]],
    help="Choose one or more columns. Matching is case/punctuation insensitive.",
)

# Enrichment columns: choose from enrichment columns; we’ll write into base (create if missing)
enrich_cols = st.multiselect(
    "Select the column(s) you want to bring over from the enrichment file",
    options=sorted(enrich_df.columns),
    default=[c for c in ["Address", "Address 2", "City", "State/Province", "Zip Code", "Employer Name",
                        "Occupation", "CountryCode", "CountryName"] if c in enrich_df.columns],
)

# Dedupe logic
st.subheader("Duplicate handling in enrichment file")
dedupe = st.checkbox("If enrichment has duplicate matches, use the most recent row by timestamp", value=True)

ts_col = None
if dedupe:
    ts_candidates = [c for c in enrich_df.columns if c.lower() in ["load_date", "loaddate", "timestamp", "updated_at", "updatedat", "last_updated", "lastupdated"]]
    ts_default = "load_date" if "load_date" in enrich_df.columns else (ts_candidates[0] if ts_candidates else None)
    ts_col = st.selectbox(
        "Timestamp column (most recent wins)",
        options=sorted(enrich_df.columns),
        index=(sorted(enrich_df.columns).index(ts_default) if ts_default in enrich_df.columns else 0),
        help="Rows are deduped within each match-key by this timestamp.",
    )

# Run
st.divider()
run = st.button("Run enrichment", type="primary", use_container_width=True)

if run:
    if not match_cols:
        st.error("Select at least one match column.")
        st.stop()
    if not enrich_cols:
        st.error("Select at least one enrichment column to copy over.")
        st.stop()

    try:
        out_df = enrich(
            base_df=base_df,
            enrich_df=enrich_df,
            match_cols=match_cols,
            enrich_cols=enrich_cols,
            overwrite=overwrite,
            dedupe=dedupe,
            ts_col=ts_col,
        )
    except Exception as e:
        st.error(f"Enrichment failed: {e}")
        st.stop()

    st.success("Done.")
    st.subheader("Output preview")
    st.write(f"Rows: {len(out_df):,} | Cols: {len(out_df.columns):,}")
    st.dataframe(out_df.head(50), use_container_width=True)

    # Downloads
    safe_stem = re.sub(r'[\\/:*?"<>|]+', "_", (output_stem or "enriched")).strip()
    safe_stem = safe_stem or "enriched"

    if output_format.startswith("Excel"):
        data = to_excel_bytes(out_df)
        st.download_button(
            "Download enriched Excel",
            data=data,
            file_name=f"{safe_stem}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )
    else:
        data = to_csv_bytes(out_df)
        st.download_button(
            "Download enriched CSV",
            data=data,
            file_name=f"{safe_stem}.csv",
            mime="text/csv",
            use_container_width=True,
        )


    # Basic diagnostics
    st.divider()
    st.subheader("Diagnostics")

    # Match rate estimate
    base_keys = make_key(base_df.copy(), match_cols)
    enrich_keys = make_key(enrich_df.copy(), match_cols)
    matched = base_keys.isin(set(enrich_keys)).sum()
    st.write(f"Base rows with at least one match in enrichment: **{matched:,} / {len(base_df):,}**")

    # Duplicates in enrichment
    dup_count = enrich_keys.duplicated().sum()
    st.write(f"Duplicate match-keys in enrichment (before dedupe): **{dup_count:,}**")
