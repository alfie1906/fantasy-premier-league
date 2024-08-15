import ssl
import sys
sys.path.insert(0, '../../')

from collections import defaultdict

import pandas as pd

# from data_ingestion.data_utils import get_github_data
ssl._create_default_https_context = ssl._create_unverified_context

def get_github_data(url: str) -> pd.DataFrame:
    """
    Function for pulling data from a GitHub CSV into a Polars DataFrame.

    Parameters
    ----------
    url : str
        The URL of the CSV file on GitHub

    Returns
    -------
    pd.DataFrame
        A Polars DataFrame containing the data from the CSV file.
    """
    url += "?raw=true"
    data = pd.read_csv(url)

    return data

def get_past_season_player_rois() -> pd.DataFrame:
    """
    Function for pulling the past season player ROI data from GitHub.

    Returns
    -------
    pd.DataFrame
        A Polars DataFrame containing the past season player ROI data.
    """
    players_end_of_season = get_github_data('https://github.com/vaastav/Fantasy-Premier-League/blob/master/data/2023-24/cleaned_players.csv')
    players_end_of_season['name'] = players_end_of_season['first_name'] + ' ' + players_end_of_season['second_name']
    players_week_1 = get_github_data('https://github.com/vaastav/Fantasy-Premier-League/blob/master/data/2023-24/gws/gw1.csv')

    player_rois = players_week_1[['name', 'team', 'value']].merge(
        players_end_of_season[['name', 'total_points']],
        on='name',
        how='outer'
    )

    player_rois['player_ROI'] = player_rois['total_points'] / player_rois['value']
    
    return player_rois

def get_upcoming_season_players() -> pd.DataFrame:
    """
    Function for getting the upcoming season player data.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the upcoming season player data. 
    """
    upcoming_season_players = get_github_data('https://github.com/vaastav/Fantasy-Premier-League/blob/master/data/2024-25/players_raw.csv')
    upcoming_season_players['name'] = upcoming_season_players['first_name'] + ' ' + upcoming_season_players['second_name']

    upcoming_season_players['position'] = upcoming_season_players['element_type'].map({
        1: 'GK',
        2: 'DEF',
        3: 'MID',
        4: 'FWD'
    })

    upcoming_season_players = upcoming_season_players[['name', 'position', 'now_cost', 'team_code', 'selected_by_percent']]

    upcoming_season_players = upcoming_season_players.merge(
        get_teams(),
        on='team_code',
        how='inner'
    )

    return upcoming_season_players

def add_past_season_rois(upcoming_season_players: pd.DataFrame, past_season_player_rois: pd.DataFrame) -> pd.DataFrame:
    """
    Function for adding the past season player ROI data to the upcoming season player data.

    Parameters
    ----------
    upcoming_season_players : pd.DataFrame
        A pandas DataFrame containing the upcoming season player data.
    past_season_player_rois : pd.DataFrame
        A pandas DataFrame containing the past season player ROI data.

    Returns
    -------
    pd.DataFrame
        A pandas DataFrame containing the upcoming season player data with the past season player ROI data added.
    """
    upcoming_season_players = upcoming_season_players.merge(
        past_season_player_rois[['name', 'player_ROI', 'total_points']],
        on='name',
        how='left'
    )

    upcoming_season_players.rename(columns={'player_ROI': 'past_season_player_ROI', 'total_points': 'past_season_total_points'}, inplace=True)
    upcoming_season_players.sort_values('past_season_player_ROI', ascending=False, inplace=True)
    

    return upcoming_season_players

def get_teams():
    teams = get_github_data('https://github.com/vaastav/Fantasy-Premier-League/blob/master/data/2024-25/teams.csv')
    teams.rename(columns={'name': 'team', 'code': 'team_code'}, inplace=True)

    return teams[['team', 'team_code']]

def team_full(team_code: int, team_counts: dict) -> bool:
    """
    Check if a team has reached the maximum limit of players.

    Parameters
    ----------
    team_code : int
        The code representing the team.
    team_counts : dict
        A dictionary containing the count of players for each team.

    Returns
    -------
    bool
        True if the team has reached the maximum limit of players, False otherwise.
    """
    TEAM_LIMIT = 3

    return team_counts[team_code] >= TEAM_LIMIT

def position_full(position: int, position_counts: dict) -> bool:
    """
    Check if a position has reached the maximum limit of players.

    Parameters
    ----------
    position : int
        The code representing the position.
    position_counts : dict
        A dictionary containing the count of players for each position.

    Returns
    -------
    bool
        True if the position has reached the maximum limit of players, False otherwise.
    """
    GK_LIMIT = 2
    DEF_LIMIT = 5
    MID_LIMIT = 5
    FWD_LIMIT = 3

    if position == 'GK':
        return position_counts[position] >= GK_LIMIT
    elif position == 'DEF':
        return position_counts[position] >= DEF_LIMIT
    elif position == 'MID':
        return position_counts[position] >= MID_LIMIT
    elif position == 'FWD':
        return position_counts[position] >= FWD_LIMIT

if __name__ == '__main__':
    past_season_player_rois = get_past_season_player_rois()
    upcoming_season_players = get_upcoming_season_players()
    upcoming_season_players = add_past_season_rois(upcoming_season_players, past_season_player_rois)
    
    team_counts = defaultdict(int)
    position_counts = defaultdict(int)
    team = []
    budget = 1000

    exclusion_list = ['Jarrad Branthwaite']
    for player_data in upcoming_season_players.itertuples():
        if team_full(player_data.team_code, team_counts):
            continue

        if position_full(player_data.position, position_counts):
            continue

        if player_data.now_cost > budget:
            continue

        if player_data.name in exclusion_list:
            continue

        team.append({
            'name': player_data.name,
            'team': player_data.team,
            'position': player_data.position,
            'cost': player_data.now_cost,
            'selected_by_percent': player_data.selected_by_percent,
            'past_season_player_ROI': player_data.past_season_player_ROI,
            'past_season_total_points': player_data.past_season_total_points
        
        })

        budget -= player_data.now_cost
        team_counts[player_data.team_code] += 1
        position_counts[player_data.position] += 1
    
    team = pd.DataFrame(team).sort_values('position', ascending=False)
    print(team)
    team.to_csv('data/24-25/starting_squad.csv', index=False)



