

import time
from datetime import datetime
import requests
import numpy as np
import pandas as pd
from pandas import json_normalize
from scipy.stats import poisson
from typing import List, Tuple, Optional, Any


# Optional Colab / gspread support: will run if environment provides google.colab
gc = None
try:
    from google.colab import auth  # type: ignore
    from google.auth import default  # type: ignore
    import gspread  # type: ignore
    # gspread_formatting imports are optional for formatting usage
    try:
        from gspread_formatting import *  # type: ignore
    except Exception:
        pass

    auth.authenticate_user()
    creds, _ = default()
    gc = gspread.authorize(creds)
except Exception:
    # Not running in Colab / gspread not available; user can call ensure_gspread_auth() to init
    gc = None  # remain None until explicitly authenticated


# Configuration
API_TOKEN = "---"  # replace with your token
BASE_COMPETITIONS_URI = "https://api.football-data.org/v4/competitions"
RATE_SLEEP = 15  # seconds (preserves previous behaviour)
COMPETITION_TEAM_COUNTS = {
    "PL": 20,
    "BL1": 18,
    "PD": 20,
    "SA": 20,
    "FL1": 18,
    "ELC": 24,
}


# --- Helpers ---------------------------------------------------------------

_session: Optional[requests.Session] = None


def get_session() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({"X-Auth-Token": API_TOKEN, "Accept-Encoding": ""})
        _session = s
    return _session


def ensure_gspread_auth() -> None:
    """
    Attempt to authenticate gspread if not already done.
    In Colab this is handled automatically above. Call this if gc is None.
    """
    global gc
    if gc is not None:
        return
    try:
        # try Colab style auth if available
        from google.colab import auth  # type: ignore
        from google.auth import default  # type: ignore
        import gspread  # type: ignore

        auth.authenticate_user()
        creds, _ = default()
        gc = gspread.authorize(creds)
    except Exception:
        gc = None


def get_standings_team_ids(competition_code: str) -> List[Tuple[int, str]]:
    """Return list of (team_id, team_name) for a competition standings."""
    url = f"{BASE_COMPETITIONS_URI}/{competition_code}/standings"
    resp = get_session().get(url)
    resp.raise_for_status()
    data = resp.json()
    df = json_normalize(data.get("standings", []))
    df2 = json_normalize(df.get("table", []))
    columns = list(df2)
    ids: List[Tuple[int, str]] = []
    for c in columns:
        team_id = df2[c][0].get("team.id")
        team_name = df2[c][0].get("team.name")
        ids.append((team_id, team_name))
    return ids


def fetch_team_matches(team_id: int) -> pd.DataFrame:
    """Fetch matches for a team and return a normalized pandas DataFrame."""
    url = f"https://api.football-data.org/v4/teams/{team_id}/matches"
    resp = get_session().get(url)
    resp.raise_for_status()
    data = resp.json()
    return json_normalize(data.get("matches", []))


def safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def compute_team_averages(df_matches: pd.DataFrame, competition_name: str, team_id: int, upto_matchday: int) -> Tuple[float, float, float, float]:
    """
    Compute (goalsScoredHomeAv, goalsConcededHomeAv, goalsScoredAwayAv, goalsConcededAwayAv)
    for a team up to (but not including) matchday.
    """
    if df_matches.empty:
        return 0.0, 0.0, 0.0, 0.0

    teamdata = df_matches[
        [
            "matchday",
            "competition.name",
            "homeTeam.id",
            "awayTeam.id",
            "score.fullTime.home",
            "score.fullTime.away",
        ]
    ]
    comp_matches = teamdata[teamdata["competition.name"] == competition_name]
    teamHomeData = comp_matches[(comp_matches["homeTeam.id"] == team_id) & (comp_matches["matchday"] < upto_matchday)]
    teamAwayData = comp_matches[(comp_matches["awayTeam.id"] == team_id) & (comp_matches["matchday"] < upto_matchday)]

    goalsScoredHome = teamHomeData["score.fullTime.home"].sum()
    goalsConcededHome = teamHomeData["score.fullTime.away"].sum()
    goalsScoredAway = teamAwayData["score.fullTime.away"].sum()
    goalsConcededAway = teamAwayData["score.fullTime.home"].sum()

    return (
        safe_div(goalsScoredHome, upto_matchday),
        safe_div(goalsConcededHome, upto_matchday),
        safe_div(goalsScoredAway, upto_matchday),
        safe_div(goalsConcededAway, upto_matchday),
    )


# --- Core generic function ------------------------------------------------


def match_day_stats(competition_code: str, competition_name: str, matchday: int) -> pd.DataFrame:
    """
    Generic implementation used by matchDayPLStats, matchDayBLStats, ...
    Returns a DataFrame with the same structure as the originals.
    """
    teams = get_standings_team_ids(competition_code)

    leagueData = []
    for team_id, _team_name in teams:
        team_matches = fetch_team_matches(team_id)
        gsHomeAv, gcHomeAv, gsAwayAv, gcAwayAv = compute_team_averages(team_matches, competition_name, team_id, matchday)
        leagueData.append([team_id, gsHomeAv, gcHomeAv, gsAwayAv, gcAwayAv])
        time.sleep(RATE_SLEEP)

    # competition matches for next-games & league averages
    url = f"{BASE_COMPETITIONS_URI}/{competition_code}/matches"
    resp = get_session().get(url)
    resp.raise_for_status()
    df_matches = json_normalize(resp.json().get("matches", []))

    nextGames = df_matches[df_matches["matchday"] == matchday]
    allTeamsGoals = df_matches[df_matches["matchday"] <= matchday]
    # restrict columns like original code
    allTeamsGoals = allTeamsGoals[["homeTeam.name", "score.fullTime.home", "score.fullTime.away", "awayTeam.name", "matchday"]]

    allTeamsAwayGoalsScored = allTeamsGoals["score.fullTime.away"].sum()
    allTeamsHomeGoalsScored = allTeamsGoals["score.fullTime.home"].sum()

    teams_count = COMPETITION_TEAM_COUNTS.get(competition_code, 20)
    allTeamsAwayGoalsScoredAv = safe_div(allTeamsAwayGoalsScored / teams_count, matchday)
    allTeamsHomeGoalsScoredAv = safe_div(allTeamsHomeGoalsScored / teams_count, matchday)

    nextGames = nextGames[["homeTeam.id", "awayTeam.id", "score.fullTime.home", "score.fullTime.away", "homeTeam.name", "awayTeam.name"]]

    homeTeams = nextGames["homeTeam.id"].tolist()
    awayTeams = nextGames["awayTeam.id"].tolist()
    homeGoals = nextGames["score.fullTime.home"].tolist()
    awayGoals = nextGames["score.fullTime.away"].tolist()
    homeTeamsNames = nextGames["homeTeam.name"].tolist()
    awayTeamsNames = nextGames["awayTeam.name"].tolist()

    leagueDatadf = pd.DataFrame(np.array(leagueData), columns=["id", "goalsScoredHomeAv", "goalsConcededHomeAv", "goalsScoredAwayAv", "goalsConcededAwayAv"])
    leagueDatadf = leagueDatadf.set_index("id")

    gameData = []
    for i in range(0, len(homeTeams)):
        idH = homeTeams[i]
        idA = awayTeams[i]
        hG = homeGoals[i]
        aG = awayGoals[i]
        nameHome = homeTeamsNames[i]
        nameAway = awayTeamsNames[i]

        # protect divisions by zero
        if allTeamsHomeGoalsScoredAv and allTeamsAwayGoalsScoredAv:
            heG = (
                leagueDatadf.loc[idH].goalsScoredHomeAv / allTeamsHomeGoalsScoredAv
                * leagueDatadf.loc[idA].goalsConcededAwayAv / allTeamsAwayGoalsScoredAv
                * allTeamsHomeGoalsScoredAv
            )
            aeG = (
                leagueDatadf.loc[idA].goalsScoredAwayAv / allTeamsAwayGoalsScoredAv
                * leagueDatadf.loc[idH].goalsConcededHomeAv / allTeamsAwayGoalsScoredAv
                * allTeamsAwayGoalsScoredAv
            )
        else:
            heG = 0.0
            aeG = 0.0

        ph0 = 1 - poisson.cdf(k=0, mu=heG)
        ph1 = 1 - poisson.cdf(k=1, mu=heG)
        ph2 = 1 - poisson.cdf(k=2, mu=heG)
        pa0 = 1 - poisson.cdf(k=0, mu=aeG)
        pa1 = 1 - poisson.cdf(k=1, mu=aeG)
        pa2 = 1 - poisson.cdf(k=2, mu=aeG)

        gameData.append([idH, idA, nameHome, nameAway, heG, aeG, ph0, ph1, ph2, pa0, pa1, pa2, hG, aG])

    cols = ["Home team id", "Away team id", "Home team", "Away team", "heg", "aeg", "+0 HG", "+1 HG", "+2 HG", "+0 AG", "+1 AG", "+2 AG", "HG", "AG"]
    return pd.DataFrame(gameData, columns=cols)


# --- Thin wrappers kept to preserve original public API --------------------


def matchDayPLStats(matchday: int) -> pd.DataFrame:
    return match_day_stats("PL", "Premier League", matchday)


def matchDayBLStats(matchday: int) -> pd.DataFrame:
    return match_day_stats("BL1", "Bundesliga", matchday)


def matchDayPDStats(matchday: int) -> pd.DataFrame:
    # competition name used in team matches
    return match_day_stats("PD", "Primera Division", matchday)


def matchDaySAStats(matchday: int) -> pd.DataFrame:
    return match_day_stats("SA", "Serie A", matchday)


def matchDayL1Stats(matchday: int) -> pd.DataFrame:
    return match_day_stats("FL1", "Ligue 1", matchday)


def matchDayPL2Stats(matchday: int) -> pd.DataFrame:
    return match_day_stats("ELC", "Championship", matchday)


# --- toHist wrapper + thin named functions to preserve API ----------------


def _to_hist_generic(match_fn, numjornada1: int, numjornada2: int, nombreHist: str) -> None:
    ensure_gspread_auth()
    if gc is None:
        raise RuntimeError("gspread not authenticated. Call ensure_gspread_auth() in Colab or set up gspread manually.")



    worksheet = gc.open(nombreHist)
    for i in range(numjornada1, numjornada2):
        sheet_name = f"Sheet{i}"
        if sheet_name not in [s.title for s in worksheet.worksheets()]:
            worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
            df = match_fn(i)
            sheet = worksheet.worksheet(sheet_name)
            sheet.update([df.columns.values.tolist()] + df.fillna(-1).values.tolist())


def toHistL1(numjornada1: int, numjornada2: int, nombreHist: str) -> None:
    _to_hist_generic(matchDayL1Stats, numjornada1, numjornada2, nombreHist)


def toHistBL(numjornada1: int, numjornada2: int, nombreHist: str) -> None:
    _to_hist_generic(matchDayBLStats, numjornada1, numjornada2, nombreHist)


def toHistPD(numjornada1: int, numjornada2: int, nombreHist: str) -> None:
    _to_hist_generic(matchDayPDStats, numjornada1, numjornada2, nombreHist)


def toHistPL(numjornada1: int, numjornada2: int, nombreHist: str) -> None:
    _to_hist_generic(matchDayPLStats, numjornada1, numjornada2, nombreHist)


def toHistPL2(numjornada1: int, numjornada2: int, nombreHist: str) -> None:
    _to_hist_generic(matchDayPL2Stats, numjornada1, numjornada2, nombreHist)


def toHistSA(numjornada1: int, numjornada2: int, nombreHist: str) -> None:
    _to_hist_generic(matchDaySAStats, numjornada1, numjornada2, nombreHist)


# --- get_next_gameweek_number (kept, minor clean) -------------------------


def get_next_gameweek_number(competition_code: str) -> Optional[int]:
    url = f"{BASE_COMPETITIONS_URI}/{competition_code}/matches"
    resp = get_session().get(url)
    if resp.status_code != 200:
        print("Error:", resp.json())
        return None

    data = resp.json()
    matches = data.get("matches", [])
    now = datetime.now()

    for match in matches:
        # utcDate examples end with 'Z', remove it to parse with fromisoformat
        match_date = datetime.fromisoformat(match["utcDate"].rstrip("Z"))
        if match_date > now:
            gameweek_number = match.get("matchday")
            print(f"Next Gameweek Number for {competition_code}: {gameweek_number}")
            return gameweek_number

    print(f"No upcoming gameweeks found for {competition_code}.")
    return None


# --- resultsFromHist and update_summary_sheet (kept, minor cleanups) -----


def resultsFromHist(
    nombre_google_sheet_games: str,
    nombre_hoja_games: str,
    nombre_google_sheet_hist: str,
    nombre_hoja_hist: str,
) -> None:
    """
    Sum columns M and N from a Games sheet and save to column K of a HIST sheet.
    (Preserves original behaviour.)
    """
    ensure_gspread_auth()
    if gc is None:
        raise RuntimeError("gspread not authenticated. Call ensure_gspread_auth().")

    try:
        games_sheet = gc.open(nombre_google_sheet_games).worksheet(nombre_hoja_games)
        hist_sheet = gc.open(nombre_google_sheet_hist).worksheet(nombre_hoja_hist)
    except Exception as e:
        print(f"Error opening sheets: {e}")
        return

    column_m = games_sheet.col_values(13)  # M
    column_n = games_sheet.col_values(14)  # N

    for i in range(1, max(len(column_m), len(column_n))):
        try:
            value_m = float(column_m[i]) if i < len(column_m) and column_m[i] else 0.0
            value_n = float(column_n[i]) if i < len(column_n) and column_n[i] else 0.0
            suma = value_m + value_n
            hist_sheet.update_cell(i + 1, 11, suma)  # K
        except Exception as e:
            print(f"Error in row {i+1}: {e}")

    print("Completed")


def update_summary_sheet(sheet_name: str) -> None:
    ensure_gspread_auth()
    if gc is None:
        raise RuntimeError("gspread not authenticated. Call ensure_gspread_auth().")

    spreadsheet = gc.open(sheet_name)
    sheets = [sheet for sheet in spreadsheet.worksheets() if sheet.title != "Summary"]
    k_values = []
    for sheet in sheets:
        data = sheet.get_all_values()
        df = pd.DataFrame(data)
        if df.shape[0] <= 1 or df.shape[1] <= 11:
            continue
        df = df.iloc[1:].reset_index(drop=True)
        filtered_df = df[(df[10].astype(str).str.strip() != "") & (df[11].astype(str).str.strip() != "")]
        k_values.extend(filtered_df[10].tolist())

    summary_sheet = spreadsheet.worksheet("Summary")
    summary_data = summary_sheet.get_all_values()
    if not summary_data:
        summary_df = pd.DataFrame(columns=[str(i) for i in range(12)])
    else:
        summary_df = pd.DataFrame(summary_data)

    if summary_df.shape[1] <= 11:
        print("The 'Summary' sheet does not have enough columns.")
        return

    total_rows_needed = len(k_values) + (len(k_values) // 3)
    while summary_df.shape[0] < total_rows_needed + 3:
        summary_df.loc[len(summary_df)] = [""] * summary_df.shape[1]

    start_row = 2
    formatted_k_values = []
    count = 0
    for value in k_values:
        formatted_k_values.append([value])
        count += 1
        if count % 3 == 0:
            formatted_k_values.append([""])

    summary_sheet.update(values=formatted_k_values, range_name=f"K{start_row}:K{start_row+len(formatted_k_values)-1}")





def safe_div(a: float, b: float) -> float:
    return a / b if b else 0.0


def get_standings_team_ids(competition_code: str) -> List[Tuple[int, str]]:
    """
    Return list[(team_id, team_name)] for competition standings.
    """
    url = f"{BASE_COMPETITIONS_URI}/{competition_code}/standings"
    resp = get_session().get(url)
    resp.raise_for_status()
    data = resp.json()
    df = json_normalize(data.get("standings", []))
    df2 = json_normalize(df.get("table", []))
    ids: List[Tuple[int, str]] = []
    for c in list(df2):
        team_id = df2[c][0].get("team.id")
        team_name = df2[c][0].get("team.name")
        ids.append((team_id, team_name))
    return ids


def fetch_team_matches(team_id: int) -> pd.DataFrame:
    url = f"https://api.football-data.org/v4/teams/{team_id}/matches"
    resp = get_session().get(url)
    resp.raise_for_status()
    return json_normalize(resp.json().get("matches", []))


def compute_team_averages(df_matches: pd.DataFrame, competition_name: str, team_id: int, upto_matchday: int):
    """
    Return (goalsScoredHomeAv, goalsConcededHomeAv, goalsScoredAwayAv, goalsConcededAwayAv)
    up to (but excluding) matchday.
    """
    if df_matches is None or df_matches.empty:
        return 0.0, 0.0, 0.0, 0.0

    teamdata = df_matches[
        [
            "matchday",
            "competition.name",
            "homeTeam.id",
            "awayTeam.id",
            "score.fullTime.home",
            "score.fullTime.away",
        ]
    ]
    comp_matches = teamdata[teamdata["competition.name"] == competition_name]
    teamHomeData = comp_matches[(comp_matches["homeTeam.id"] == team_id) & (comp_matches["matchday"] < upto_matchday)]
    teamAwayData = comp_matches[(comp_matches["awayTeam.id"] == team_id) & (comp_matches["matchday"] < upto_matchday)]

    goalsScoredHome = teamHomeData["score.fullTime.home"].sum()
    goalsConcededHome = teamHomeData["score.fullTime.away"].sum()
    goalsScoredAway = teamAwayData["score.fullTime.away"].sum()
    goalsConcededAway = teamAwayData["score.fullTime.home"].sum()

    return (
        safe_div(goalsScoredHome, upto_matchday),
        safe_div(goalsConcededHome, upto_matchday),
        safe_div(goalsScoredAway, upto_matchday),
        safe_div(goalsConcededAway, upto_matchday),
    )


def match_day_stats(competition_code: str, competition_name: str, matchday: int) -> pd.DataFrame:
    """
    Generic match-day stats generator. Returns DataFrame with columns:
    ['Home team id','Away team id','Home team','Away team','heg','aeg','+0 HG','+1 HG','+2 HG','+0 AG','+1 AG','+2 AG','HG','AG']
    """
    teams = get_standings_team_ids(competition_code)
    leagueData = []
    for team_id, _team_name in teams:
        matches = fetch_team_matches(team_id)
        gsHomeAv, gcHomeAv, gsAwayAv, gcAwayAv = compute_team_averages(matches, competition_name, team_id, matchday)
        leagueData.append([team_id, gsHomeAv, gcHomeAv, gsAwayAv, gcAwayAv])
        time.sleep(RATE_SLEEP)

    # competition matches
    url = f"{BASE_COMPETITIONS_URI}/{competition_code}/matches"
    resp = get_session().get(url)
    resp.raise_for_status()
    df_matches = json_normalize(resp.json().get("matches", []))

    nextGames = df_matches[df_matches["matchday"] == matchday]
    allTeamsGoals = df_matches[df_matches["matchday"] <= matchday]
    allTeamsGoals = allTeamsGoals[["homeTeam.name", "score.fullTime.home", "score.fullTime.away", "awayTeam.name", "matchday"]]

    allTeamsAwayGoalsScored = allTeamsGoals["score.fullTime.away"].sum()
    allTeamsHomeGoalsScored = allTeamsGoals["score.fullTime.home"].sum()

    teams_count = COMPETITION_TEAM_COUNTS.get(competition_code, 20)
    allTeamsAwayGoalsScoredAv = safe_div(allTeamsAwayGoalsScored / teams_count, matchday)
    allTeamsHomeGoalsScoredAv = safe_div(allTeamsHomeGoalsScored / teams_count, matchday)

    nextGames = nextGames[["homeTeam.id", "awayTeam.id", "score.fullTime.home", "score.fullTime.away", "homeTeam.name", "awayTeam.name"]]

    homeTeams = nextGames["homeTeam.id"].tolist()
    awayTeams = nextGames["awayTeam.id"].tolist()
    homeGoals = nextGames["score.fullTime.home"].tolist()
    awayGoals = nextGames["score.fullTime.away"].tolist()
    homeTeamsNames = nextGames["homeTeam.name"].tolist()
    awayTeamsNames = nextGames["awayTeam.name"].tolist()

    leagueDatadf = pd.DataFrame(np.array(leagueData), columns=["id", "goalsScoredHomeAv", "goalsConcededHomeAv", "goalsScoredAwayAv", "goalsConcededAwayAv"]).set_index("id")

    gameData = []
    for i in range(len(homeTeams)):
        idH = homeTeams[i]
        idA = awayTeams[i]
        hG = homeGoals[i]
        aG = awayGoals[i]
        nameHome = homeTeamsNames[i]
        nameAway = awayTeamsNames[i]

        if allTeamsHomeGoalsScoredAv and allTeamsAwayGoalsScoredAv:
            heG = leagueDatadf.loc[idH].goalsScoredHomeAv / allTeamsHomeGoalsScoredAv * leagueDatadf.loc[idA].goalsConcededAwayAv / allTeamsAwayGoalsScoredAv * allTeamsHomeGoalsScoredAv
            aeG = leagueDatadf.loc[idA].goalsScoredAwayAv / allTeamsAwayGoalsScoredAv * leagueDatadf.loc[idH].goalsConcededHomeAv / allTeamsAwayGoalsScoredAv * allTeamsAwayGoalsScoredAv
        else:
            heG = 0.0
            aeG = 0.0

        ph0 = 1 - poisson.cdf(k=0, mu=heG)
        ph1 = 1 - poisson.cdf(k=1, mu=heG)
        ph2 = 1 - poisson.cdf(k=2, mu=heG)
        pa0 = 1 - poisson.cdf(k=0, mu=aeG)
        pa1 = 1 - poisson.cdf(k=1, mu=aeG)
        pa2 = 1 - poisson.cdf(k=2, mu=aeG)

        gameData.append([idH, idA, nameHome, nameAway, heG, aeG, ph0, ph1, ph2, pa0, pa1, pa2, hG, aG])

    cols = ["Home team id", "Away team id", "Home team", "Away team", "heg", "aeg", "+0 HG", "+1 HG", "+2 HG", "+0 AG", "+1 AG", "+2 AG", "HG", "AG"]
    return pd.DataFrame(gameData, columns=cols)


# Thin wrappers to preserve original API
def matchDaySAStats(matchday): return match_day_stats("SA", "Serie A", matchday)
def matchDayBLStats(matchday): return match_day_stats("BL1", "Bundesliga", matchday)
def matchDayPDStats(matchday): return match_day_stats("PD", "Primera Division", matchday)
def matchDayPLStats(matchday): return match_day_stats("PL", "Premier League", matchday)
def matchDayL1Stats(matchday): return match_day_stats("FL1", "Ligue 1", matchday)
def matchDayPL2Stats(matchday): return match_day_stats("ELC", "Championship", matchday)


# Google Sheets helpers
def get_sheet_id_by_name(sheet_name: str) -> str:
    """
    Return Google Drive file id for first file matching sheet_name.
    """
    ensure_gspread_auth()
    if gc is None:
        raise RuntimeError("gspread not authenticated")
    # Use Drive API to find file
    from google.auth import default as ga_default  # type: ignore
    from googleapiclient.discovery import build as apiclient_build  # type: ignore

    creds, _ = ga_default()
    drive_service = apiclient_build("drive", "v3", credentials=creds)
    results = drive_service.files().list(q=f"name='{sheet_name}'").execute()
    items = results.get("files", [])
    if items:
        return items[0]["id"]
    raise ValueError(f"No file named '{sheet_name}' found")


def read_google_sheets_into_dataframe(sheet_names: List[str]) -> pd.DataFrame:
    """
    Read all worksheets from the given Google Sheets (by name) and concatenate into a DataFrame.
    """
    ensure_gspread_auth()
    if gc is None:
        raise RuntimeError("gspread not authenticated")
    dfs = []
    for sheet_name in sheet_names:
        sheet_id = get_sheet_id_by_name(sheet_name)
        workbook = gc.open_by_key(sheet_id)
        for worksheet in workbook.worksheets():
            data = worksheet.get_all_values()
            if not data:
                continue
            df = pd.DataFrame(data[1:], columns=data[0])
            dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# ML preparation / training (kept top-level behavior similar to original)
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error

def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    return df[["+0 HG", "+1 HG", "+2 HG", "+0 AG", "+1 AG", "+2 AG"]]

def assign_target(df: pd.DataFrame) -> pd.Series:
    return (pd.to_numeric(df["HG"], errors="coerce").fillna(0) + pd.to_numeric(df["AG"], errors="coerce").fillna(0))

# Train a global model if possible (preserve previous top-level training)
clf: Optional[DecisionTreeRegressor] = None
try:
    # attempt to read sheets used originally
    sheet_names = ["PLGames2526", "SAGames2526", "PDGames2526", "L1Games2526", "BLGames2526", "PL2Games2526"]
    final_df = read_google_sheets_into_dataframe(sheet_names)
    if not final_df.empty:
        df = final_df[["Home team id", "Away team id", "+0 HG", "+1 HG", "+2 HG", "+0 AG", "+1 AG", "+2 AG", "HG", "AG"]]
        df = df.drop(df[df.eq("nan").any(axis=1)].index)
        for col in ["Home team id", "Away team id", "HG", "AG", "+0 HG", "+1 HG", "+2 HG", "+0 AG", "+1 AG", "+2 AG"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors="coerce").fillna(0)
        y = assign_target(df)
        X = prepare_features(df)
        if not X.empty:
            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
            clf = DecisionTreeRegressor()


            clf.fit(X_train, y_train)

            y_pred = clf.predict(X_test)
            mae = mean_absolute_error(y_test, y_pred)
            # mse variable preserved if needed
            mse = mean_squared_error(y_test, y_pred)
            print("DecisionTree trained. MAE:", mae)
except Exception as e:
    # keep failures non-fatal at import time
    print("Warning: initial model training failed:", e)
    clf = None


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    return df[["+0 HG", "+1 HG", "+2 HG", "+0 AG", "+1 AG", "+2 AG"]]


def alg2(sheetname: List[str], df: pd.DataFrame):
    """
    Read provided sheet names, train model (or reuse global clf) and predict for df.
    Returns predictions array.
    """
    final_df = read_google_sheets_into_dataframe(sheetname)
    if final_df.empty:
        raise RuntimeError("No data found for alg2 training")
    data = final_df[["Home team id", "Away team id", "+0 HG", "+1 HG", "+2 HG", "+0 AG", "+1 AG", "+2 AG", "HG", "AG"]]
    data = data.drop(data[data.eq("nan").any(axis=1)].index)
    for col in ["Home team id", "Away team id", "HG", "AG", "+0 HG", "+1 HG", "+2 HG", "+0 AG", "+1 AG", "+2 AG"]:
        if col in data.columns:
            data[col] = pd.to_numeric(data[col].astype(str).str.replace(',', '.'), errors="coerce").fillna(0)
    y = assign_target(data)
    X = prepare_features(data)
    model = DecisionTreeRegressor()


    if not X.empty:
        model.fit(X, y)

    pred_df = prepare(df)
    return model.predict(pred_df)


def gamesToHist(f, gameWeek: int, sheet_names: List[str], histName: str, pageNum: int):
    """
    Generate predictions for a fixture function f, combine predictions and upload to histName sheet.
    If the Google Sheet (spreadsheet) named `histName` does not exist, it will be created
    inside the folder named "25-26".
    """
    df = f(gameWeek)
    dfPred = prepare(df)
    if clf is None:
        raise RuntimeError("Global model not trained; alg2 or local training required")

    y_pred1 = clf.predict(dfPred)
    y_pred2 = alg2(sheet_names, dfPred)

    df = df.drop(
        columns=[c for c in ["Home team id", "Away team id", "heg", "aeg", "HG", "AG"] if c in df.columns],
        errors="ignore"
    )

    df["A1"] = y_pred1
    df["A2"] = y_pred2
    df["Result"] = ""
    df["Bet"] = ""

    ensure_gspread_auth()
    if gc is None:
        raise RuntimeError("gspread not authenticated")


    drive = GoogleDriveClient()
    folder_list = drive.list_folders_by_name("25-26")

    if not folder_list:
        raise RuntimeError("Folder '25-26' not found in your Google Drive")

    folder_id = folder_list[0]["id"]

    try:
        worksheet = gc.open(histName)
    except gspread.SpreadsheetNotFound:
        print(f"Spreadsheet '{histName}' not found. Creating inside folder '25-26'...")

        # Create the sheet *inside the folder*
        worksheet = gc.create(histName, folder_id=folder_id)

        from google.auth import default as ga_default
        creds, _ = ga_default()
        user_email = creds.service_account_email
        worksheet.share(user_email, perm_type='user', role='writer')

    sheet_name = f"Sheet{pageNum}"

    try:
        sheet = worksheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        sheet = worksheet.worksheet(sheet_name)

    sheet.update([df.columns.values.tolist()] + df.fillna(-1).values.tolist())



def paint_result(spreadsheet_name: str):
    """
    Paint 'Bet' column light green when abs(A1-A2) < 2 for all worksheets.
    """
    ensure_gspread_auth()
    if gc is None:
        raise RuntimeError("gspread not authenticated")
    spreadsheet = gc.open(spreadsheet_name)
    worksheets = spreadsheet.worksheets()
    for sheet in worksheets:
        try:
            header_row = sheet.row_values(1)
            a1_index = header_row.index("A1")
            a2_index = header_row.index("A2")
            bet_index = header_row.index("Bet")
            values = sheet.get_all_values()
            for i in range(1, len(values)):
                try:
                    value_a1 = float(values[i][a1_index])
                    value_a2 = float(values[i][a2_index])
                    if abs(value_a1 - value_a2) < 2:
                        cell = f"{chr(ord('A') + bet_index)}{i + 1}"
                        sheet.format(cell, {"backgroundColor": {"red": 0.9, "green": 1, "blue": 0.9}})
                except (ValueError, IndexError):
                    # skip invalid numeric or missing columns
                    continue
        except ValueError:
            continue
        except Exception as e:
            if "Quota exceeded" in str(e):
                time.sleep(60)
            else:
                print("Error processing sheet:", e)
def get_next_gameweek_number(competition_code: str) -> Optional[int]:
    url = f"{BASE_COMPETITIONS_URI}/{competition_code}/matches"
    resp = get_session().get(url)
    if resp.status_code != 200:
        print("Error:", resp.json())
        return None

    data = resp.json()
    matches = data.get("matches", [])
    now = datetime.now()

    for match in matches:
        # utcDate examples end with 'Z', remove it to parse with fromisoformat
        match_date = datetime.fromisoformat(match["utcDate"].rstrip("Z"))
        if match_date > now:
            gameweek_number = match.get("matchday")
            print(f"Next Gameweek Number for {competition_code}: {gameweek_number}")
            return gameweek_number

    print(f"No upcoming gameweeks found for {competition_code}.")
    return None


def process_bet_column_by_name(spreadsheet_name: str):
    """
    Update 'Bet' column to '+1' or '-4' based on A1/A2 and copy qualifying rows to 'Summary'.
    """
    ensure_gspread_auth()
    if gc is None:
        raise RuntimeError("gspread not authenticated")
    from google.auth import default as ga_default  # type: ignore
    creds, _ = ga_default()
    # Re-authorize local client to ensure write permissions
    local_gc = gc

    try:
        spreadsheet = local_gc.open(spreadsheet_name)
    except Exception:
        raise ValueError(f"Spreadsheet {spreadsheet_name} not found")

    # Create or clear Summary sheet
    try:
        summary_sheet = spreadsheet.worksheet("Summary")
        summary_sheet.clear()
    except Exception:
        summary_sheet = spreadsheet.add_worksheet(title="Summary", rows="1000", cols="26")
    summary_sheet.append_row([f"Column {chr(65 + i)}" for i in range(26)])

    worksheets = spreadsheet.worksheets()
    row_counter = 0
    rc = 0

    for sheet in worksheets:
        if sheet.title == "Summary":
            continue
        try:
            header_row = sheet.row_values(1)
            a1_index = header_row.index("A1")
            a2_index = header_row.index("A2")
            bet_index = header_row.index("Bet")
            values = sheet.get_all_values()
            for i in range(1, len(values)):
                try:
                    value_a1 = float(values[i][a1_index])
                    value_a2 = float(values[i][a2_index])
                    if abs(value_a1 - value_a2) < 2 and (value_a1 + value_a2) >= 5:
                        sheet.update_cell(i + 1, bet_index + 1, "'+1")
                        summary_sheet.append_row(values[i])
                        row_counter += 1
                        rc += 1
                    elif abs(value_a1 - value_a2) < 2 and (value_a1 + value_a2) < 5:
                        if value_a1 == 2 and value_a2 == 2:
                            continue
                        else:
                            sheet.update_cell(i + 1, bet_index + 1, "'-4")
                            summary_sheet.append_row(values[i])
                            row_counter += 1
                            rc += 1
                    if row_counter == 3:
                        summary_sheet.append_row(["-"] )
                        row_counter = 0
                except ValueError:
                    continue
        except Exception as e:
            if "Quota exceeded" in str(e):
                time.sleep(60)
            else:
                print("Skipping sheet due to error:", e)
    print(f"Processed all sheets. Summary updated. Total matches: {rc}")

