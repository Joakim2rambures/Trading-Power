#smard_fetch.py
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
    SMARD data is pulled from the public SMARD API
    The varibales are start/end date, region where to pull from, and the filter id : lsit of filter id's is available on the read me file
    You can have the market prices for all countries in the europe except for the UK.
    Among the data available there is market prices, energy forecast 
    """
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    headers = {"User-Agent": "Mozilla/5.0"}

    # parse start/end (accept str or datetime); timestamps are UTC ms
    if isinstance(start, str):
        start = pd.to_datetime(start, utc=True) # if start is a string set start = start 
    if isinstance(end, str): 
        end = pd.to_datetime(end, utc=True) # if end is a string set end=end  
    if end < start:
        raise ValueError("end must be >= start") 

    start_ms = int(start.timestamp() * 1000) # here we transofrm the start and end date to match smard (unix milliseconds)
    end_ms = int(end.timestamp() * 1000)

    # 1) list chunk timestamps
    idx = requests.get(
        f"{base}/{filter_id}/{region}/index_{resolution}.json",
        headers=headers,
        timeout=60,
        verify=verify
    ).json() 

    # requests.get() comes form the library requests and will send a request to the API (in this case teh SMARD API) and sends a GET requests (want to get some data)
    # .json() will grab the data we fetched which grabs the teh JSOn data in the python dict format
    # this first request only contains the timestamps (whic have the form 175952600000, 17958469983,...)

    stamps = sorted(idx.get("timestamps", [])) #.get() is different from the one above, this one returns the item() corresponding to the key timestamp in a dictionnary (or else if dict empy return [])
    if not stamps:
        return pd.DataFrame(columns=["time_utc", "value"]) 

    # 2) choose the chunks that cover [start, end]
    ms_index = bisect.bisect_right(stamps, start_ms) - 1  
    # last stamp <= start. bisect_right - 1 finds the index where the first data ( = start).if we have the same values multiple times, it pulls the latests one. 
    ms_index = max(ms_index, 0)
    selected = [s for s in stamps[ms_index:] if s <= end_ms] # : grabs and stores all the dates we want in timestamps until end_date 
    if not selected and stamps[ms_index] <= end_ms:
        selected = [stamps[ms_index]]  # at least include the chunk containing start

    # 3) fetch, merge, dedupe (last value wins)
    rows = []
    for time_series in selected:
        api_request = requests.get(
            f"{base}/{filter_id}/{region}/{filter_id}_{region}_{resolution}_{time_series}.json",
            headers=headers,
            timeout=60,
            verify=verify,
        ).json()
        rows += api_request.get("series") or api_request.get("series2") or [] 
        
        # Makes a second API request and gets all of the time series data based ont eh timestamps we extracted and transformed in actual time series value before
        #here each j is the request, series is the key and .get(series) returns the value (in this case tuple made of the timestamp and actual data point)

    # rows is a list of [timestamp, value]
    df = pd.DataFrame(rows, columns=["epoch_ms", "value"])

    # normalize timestamps to integer ms (like int(t))
    df["epoch_ms"] = df["epoch_ms"].astype("int64")

    # keep last value for each timestamp, then sort
    df = df.drop_duplicates(subset="epoch_ms", keep="last")
    df = df.sort_values("epoch_ms").reset_index(drop=True)

    # add datetime column
    df["time_utc"] = pd.to_datetime(df["epoch_ms"], unit="ms", utc=True)

    # 4) precise time window filter
    m = (df["epoch_ms"] >= start_ms) & (df["epoch_ms"] <= end_ms) #subsets only the period we're interested about 
    return df.loc[m, ["time_utc", "value"]].reset_index(drop=True)
# %%
