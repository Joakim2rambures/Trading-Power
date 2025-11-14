# %%
import requests, pandas as pd, urllib3, bisect
from datetime import datetime, timezone

def smard_range(
    filter_id: str = 410,
    region: str = "DE",
    resolution: str = "quarterhour",
    start="2025-11-01",
    end="2025-11-11",
    base="https://www.smard.de/app/chart_data",
    verify=False,
):
    """
    Fetch SMARD time-series into a DataFrame with columns: time_utc, value.
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    headers = {"User-Agent": "Mozilla/5.0"}

    # parse start/end (accept str or datetime); timestamps are UTC ms
    if isinstance(start, str):
        start = pd.to_datetime(start, utc=True)
    if isinstance(end, str):
        end = pd.to_datetime(end, utc=True)
    if end < start:
        raise ValueError("end must be >= start")

    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    # 1) list chunk timestamps
    idx = requests.get(
        f"{base}/{filter_id}/{region}/index_{resolution}.json",
        headers=headers,
        timeout=60,
        verify=verify,
    ).json()
    stamps = sorted(idx.get("timestamps", []))
    if not stamps:
        return pd.DataFrame(columns=["time_utc", "value"])

    # 2) choose the chunks that cover [start, end]
    i = bisect.bisect_right(stamps, start_ms) - 1  # last stamp <= start
    i = max(i, 0)
    selected = [s for s in stamps[i:] if s <= end_ms]
    if not selected and stamps[i] <= end_ms:
        selected = [stamps[i]]  # at least include the chunk containing start

    # 3) fetch, merge, dedupe (last value wins)
    rows = []
    for ts in selected:
        j = requests.get(
            f"{base}/{filter_id}/{region}/{filter_id}_{region}_{resolution}_{ts}.json",
            headers=headers,
            timeout=60,
            verify=verify,
        ).json()
        rows += j.get("series") or j.get("series2") or []

    if not rows:
        return pd.DataFrame(columns=["time_utc", "value"])

    data = sorted({int(t): v for t, v in rows}.items())
    df = pd.DataFrame(data, columns=["epoch_ms", "value"])
    df["time_utc"] = pd.to_datetime(df["epoch_ms"], unit="ms", utc=True)

    # 4) precise time window filter
    m = (df["epoch_ms"] >= start_ms) & (df["epoch_ms"] <= end_ms)
    return df.loc[m, ["time_utc", "value"]].reset_index(drop=True)
# %%
