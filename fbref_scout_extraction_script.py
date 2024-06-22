import os
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from concurrent.futures import ThreadPoolExecutor

def reset_columns(df):
    """
    Resets the columns of a DataFrame with multi-level column indexing and fills NaN values with 0.

    Parameters:
    df (pd.DataFrame): The input DataFrame with multi-level column indexing.

    Returns:
    pd.DataFrame: The modified DataFrame with single-level column names and no NaN values.
    """
    df.columns = [' '.join(col).strip() for col in df.columns]
    df = df.reset_index(drop=True)
    new_columns = []
    for col in df.columns:
        if 'level_0' in col:
            new_col = col.split()[-1]  # Takes the last name
        else:
            new_col = col
        new_columns.append(new_col)
    df.columns = new_columns
    df = df.fillna(0)
    return df
 

def clean_text(cell):
    """
    Cleans the text in an HTML cell by joining all stripped strings.

    Parameters:
    cell (BeautifulSoup element): The HTML cell element to clean.

    Returns:
    str: A single string with all stripped strings from the cell joined together.
    """
    return ' '.join(cell.stripped_strings)

def get_tables_html(url):
    """
    Retrieves all HTML tables from a given URL and returns them as a list of DataFrames.

    Parameters:
    url (str): The URL of the webpage containing HTML tables.

    Returns:
    list: A list of DataFrames containing the HTML tables. If an error occurs, an empty list is returned.
    """
    try:
        tables = pd.read_html(url)
        return tables
    except Exception as e:
        print(f"Error retrieving HTML tables from {url}: {e}")
        return []

def scrape_team_stats(team_link, base_dir):
    """
    Scrapes team statistics from a given URL and saves them as CSV files in the specified directory.


    Parameters:
    team_link (str): The URL of the team's statistics page.
    base_dir (str): The base directory where the team statistics will be saved.

    Returns:
    None
    """
    try:
        full_stats = get_tables_html(team_link)
        
        if not full_stats:
            print(f"No tables found for {team_link}")
            return
        
        team_name = team_link.split('/')[-1].split('-Stats')[0].replace("-", " ")  # Extract the team name from the URL
        
        # Path for the team's directory
        team_dir = os.path.join(base_dir, team_name)
        os.makedirs(team_dir, exist_ok=True)
        
        # Obtain and save each table in CSV format
        tables = [
            'players', 'matches', 'goalkeepers', 'advanced_goalkeeping', 'shooting', 
            'passing', 'pass_types', 'g_e_s_creation', 'defensive_actions', 
            'possession', 'playing_time', 'miscellaneous_stats'
        ]
        
        for i, table_name in enumerate(tables):
            if i < len(full_stats):
                df = reset_columns(full_stats[i])
                output_csv = os.path.join(team_dir, f"{table_name}.csv")
                df.to_csv(output_csv, index=False)
            else:
                print(f"Table {table_name} not found for {team_name}")

        print(f"Data saved for {team_name}")
        
    except Exception as e:
        print(f"Error processing the link {team_link}: {e}")


def get_team_links(league_url, number_of_team):
    """
    Retrieves the URLs for the team statistics pages from a given league URL.

    Parameters:
    league_url (str): The URL of the league's statistics page.
    number_of_team (int): The number of team links to retrieve.

    Returns:
    list: A list of URLs pointing to the statistics pages of the teams. If an error occurs, an empty list is returned.
    """
    try:
        res = Request(league_url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'})
        league_html_page = urlopen(res).read()
        soup_league = BeautifulSoup(league_html_page, 'html.parser')
        team_links = []
        team_code_name = []
        
        for link in soup_league.find_all("a", href=True):
            if "/squads/" in link['href'] and link['href'].split('/squads/')[1] not in team_code_name and len(team_code_name) < number_of_team and link['href'].split('/squads/')[1] != "":
                team_code_name.append(link['href'].split('/squads/')[1])
                team_links.append("https://fbref.com" + link['href'])
                
        return team_links
    except Exception as e:
        print(f"Error retrieving team links from {league_url}: {e}")
        return []


def scrapeStats(league, league_url, num_teams):
    """
    Scrapes statistics for all teams in a given league and saves them as CSV files in the specified directory.

    Parameters:
    league (str): The name of the league to create the folder.
    league_url (str): The URL of the league's statistics page on FBref.
    num_teams (int): The number of teams in the league.

    Returns:
    None
    """
    # Get team links
    team_links = get_team_links(league_url, num_teams)

    if not team_links:
        print("No team links found.")
        return

    # Ask for season input
    season = input("Enter the season (e.g., 2023-24): ")
    
    # Main path for the season
    base_dir = os.path.join(season, league)
    os.makedirs(base_dir, exist_ok=True)
    
    # Implement multithreading to speed up the process
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(scrape_team_stats, team_link, base_dir) for team_link in team_links]
        for future in futures:
            future.result()  # Wait for each thread to complete





def save_league_stats(league_name, url):
    """
    Scrapes league statistics from a given URL and saves them as a CSV file in the specified directory.

    Parameters:
    league_name (str): The name of the league.
    url (str): The URL of the league's statistics page.

    Returns:
    None
    """
    # Read the table from the webpage
    try:
        df = pd.read_html(url)[0]
    except Exception as e:
        print(f"Error reading the table from the webpage: {e}")
        return
    
    # Create a DataFrame with the same headers but without multi-indexing
    df.columns = [' '.join(col).strip() for col in df.columns]

    # Reset the DataFrame index
    df = df.reset_index(drop=True)

    # Create a list with new column names
    new_columns = []
    for col in df.columns:
        if 'level_0' in col:
            new_col = col.split()[-1]  # Take the last name
        else:
            new_col = col
        new_columns.append(new_col)

    # Rename columns
    df.columns = new_columns
    df = df.fillna(0)

    # Create the save path
    base_dir = os.path.join("2023-2024", league_name)
    os.makedirs(base_dir, exist_ok=True)

    # Save the DataFrame to a CSV file
    output_csv = os.path.join(base_dir, f'{league_name}_stats.csv')
    df.to_csv(output_csv, index=False)
    print(f"Data saved to {output_csv}")

import os
import pandas as pd

def combine_player_stats(base_dir="2023-2024"):
    """
    Combines 'players.csv' files from specified league directories into a single CSV file.

    Parameters:
    base_dir (str): The base directory containing league folders. Default is "2023-2024".

    Returns:
    None
    """
    leagues = ["EPL", "Serie A", "Bundesliga", "Ligue 1", "La Liga"]
    combined_df = pd.DataFrame()

    for league in leagues:
        league_path = os.path.join(base_dir, league)
        if not os.path.exists(league_path):
            print(f"League directory {league_path} does not exist. Skipping...")
            continue
        
        for team_folder in os.listdir(league_path):
            team_path = os.path.join(league_path, team_folder)
            players_csv_path = os.path.join(team_path, "players.csv")
            if os.path.isfile(players_csv_path):
                try:
                    df = pd.read_csv(players_csv_path)
                    # Drop rows where "Player" contains "Squad Total" or "Opponent Total"
                    df = df[~df['Player'].str.contains("Squad Total|Opponent Total", case=False, na=False)]
                    df["League"] = league  # Add a column to indicate the league
                    df["Team"] = team_folder  # Add a column to indicate the team
                    combined_df = pd.concat([combined_df, df], ignore_index=True)
                except Exception as e:
                    print(f"Error reading {players_csv_path}: {e}")
    
    # Save the combined DataFrame to a CSV file
    output_csv = os.path.join(base_dir, "combined_players_stats.csv")
    combined_df.to_csv(output_csv, index=False)
    print(f"Combined data saved to {output_csv}")

def combine_player_stats_singleleague(league,base_dir="2023-2024"):
    """
    Combines 'players.csv' files from specified league directories into a single CSV file.

    Parameters:
    base_dir (str): The base directory containing league folders. Default is "2023-2024".
    league (str): league name, the same used for extraction

    Returns:
    None
    """
    combined_df = pd.DataFrame()

    league_path = os.path.join(base_dir, league)
    if not os.path.exists(league_path):
        print(f"League directory {league_path} does not exist.")
        return
        
    for team_folder in os.listdir(league_path):
        team_path = os.path.join(league_path, team_folder)
        players_csv_path = os.path.join(team_path, "players.csv")
        if os.path.isfile(players_csv_path):
            try:
                df = pd.read_csv(players_csv_path)
                    # Drop rows where "Player" contains "Squad Total" or "Opponent Total"
                df = df[~df['Player'].str.contains("Squad Total|Opponent Total", case=False, na=False)]
                df["League"] = league  # Add a column to indicate the league
                df["Team"] = team_folder  # Add a column to indicate the team
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            except Exception as e:
                print(f"Error reading {players_csv_path}: {e}")
    # Save the combined DataFrame to a CSV file
    output_dir = os.path.join(base_dir, league)  
    os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists
    output_csv = os.path.join(output_dir, f"combined_players_stats_{league}.csv")
    combined_df.to_csv(output_csv, index=False)
    print(f"Combined data saved to {output_csv}")



if __name__ == "__main__":
    # save_league_stats("Ligue 1", "https://fbref.com/en/comps/13/Ligue-1-Stats")
    scrapeStats("Segunda Division", "https://fbref.com/en/comps/17/Segunda-Division-Stats", 22)
    # combine_player_stats()
    combine_player_stats_singleleague(league="Segunda Division")
