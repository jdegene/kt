# -*- coding: utf-8 -*-
# ML application to predict kicktipp results

import requests
import pandas as pd
import pendulum
import re

from bs4 import BeautifulSoup
from selenium import webdriver


# Part 1 - Get historic data using openligadb.de
def getUrl(url):

    r = requests.get(url)    
    return r.json()


def getAllGames(season, league='bl1', exstDF=None):
    """
    Gets all games of specified season, appends to exstDF if not None
    
    :season: int of season, eg 2016 is season 2016/17
    :league: league to query, tested: bl1, bl2
    :exstDF: exsiting DF to append data to, if None new one is created
    """
    
    # define column list
    collist = ['Retrieve_Date', 'Season', 'League', 'GameDay',
                                      'MatchDateTime', 'MatchDateTimeUTC',
                                      'MatchID', 'Team1_Name', 'Team1_ID', 'Team2_Name', 'Team2_ID',
                                      'Goals_Team1_FH', 'Goals_Team2_FH',
                                      'Goals_Team1_End', 'Goals_Team2_End',
                                      'Location_City', 'Location_ID', 'LocationStadium',
                                      'NumberOfViewers', 'GoalsList']
    
    # create new output DF if none was passed
    if exstDF is None:
        outDF = pd.DataFrame(columns=collist)
    else:
        outDF =  exstDF
    
    raw_json = getUrl("https://www.openligadb.de/api/getmatchdata/{}/{}".format(league, season))
    
    # add data to DataFrame
    for game in raw_json:       
    
        # define dict to 
        try:
            locCity = game['Location']['LocationCity']
        except:
            locCity = ""
        
        try:
            locID = game['Location']['LocationID']
        except:
            locID = ""
        
        try:
            locStad = game['Location']['LocationStadium']
        except:
            locStad = ""
        
        # identify end goals and goals after first half position in json
        if game['MatchResults'][0]['ResultName'] == 'Endergebnis':            
            end_result_order = 0
            fh_result_order = 1
        else:
            end_result_order = 1
            fh_result_order = 0
        
        try:
            Goals_Team1_FH = game['MatchResults'][fh_result_order]['PointsTeam1']
            Goals_Team2_FH = game['MatchResults'][fh_result_order]['PointsTeam2']
        except:
            Goals_Team1_FH = ""
            Goals_Team2_FH = ""
        
        try:
            Goals_Team1_end = game['MatchResults'][end_result_order]['PointsTeam1']
            Goals_Team2_end = game['MatchResults'][end_result_order]['PointsTeam2']
        except:
            Goals_Team1_end = ""
            Goals_Team2_end = ""
        

            
        
        outDF = outDF.append({'Retrieve_Date' :  pendulum.now().to_date_string(),
                              'Season' : season, 
                              'League' : league, 
                              'GameDay' : game['Group']['GroupOrderID'],
                              'MatchDateTime' : game['MatchDateTime'], 
                              'MatchDateTimeUTC' : game['MatchDateTimeUTC'],
                              'MatchID' : game['MatchID'], 
                              'Team1_Name' : game['Team1']['TeamName'], 
                              'Team1_ID' : game['Team1']['TeamId'], 
                              'Team2_Name' : game['Team2']['TeamName'], 
                              'Team2_ID' : game['Team2']['TeamId'],
                              'Goals_Team1_FH' : Goals_Team1_FH, 
                              'Goals_Team2_FH' : Goals_Team2_FH,
                              'Goals_Team1_End' : Goals_Team1_end, 
                              'Goals_Team2_End' : Goals_Team2_end,
                              'Location_City' : locCity, 
                              'Location_ID' : locID, 
                              'LocationStadium' :  locStad,
                              'NumberOfViewers' : game['NumberOfViewers'], 
                              'GoalsList' : game['Goals']}, 
                              ignore_index=True)
    
    # cut off not needed columns (drops not needed index columns)
    outDF = outDF[collist]    
    # drop duplicates in case of double fetched data - dont use GoalsList col, as lists are not supported for drop_duplicates
    outDF.drop_duplicates(subset=collist[:-1], inplace=True)
    
    return outDF
    

    
def buildCSV(season, league, inCsvFile):
    """
    Build a large csv File of game results
    
    :season: int of season, eg 2016 is season 2016/17
    :league: league to query, tested: bl1, bl2    
    :inFile: existing filepath to csv File, if not existing, new file is created
    """    
    
    try:
        outDF = pd.read_csv(inCsvFile, sep=";", encoding="utf8")
        outDF = getAllGames(season, league=league, exstDF=outDF)
    except:
        outDF = getAllGames(season, league=league)
    
    outDF.to_csv(inCsvFile, sep=";")
        


def fix_season(inSeason):  
    """
    Fixes the problem when season is 2009 that python correctly uses 09 instead of 9
    
    :inSeason: int season eg 9
    """    
    if inSeason < 10:
        return "0" + str(inSeason)    
    else:
        return str(inSeason)


def getCurrentSeason():
    """
    automatically determine current season, dates after July are new season, before old
    """
    today = pendulum.now().to_date_string()
    if int(today[5:7]) < 8:
        cur_season = int(today[2:4]) - 1
    else:
        cur_season = int(today[2:4])
    return cur_season


def getTableFromKicker(season, league, gameday, tableCSV):
    """
    Build a DF for each GameDay's table
    
    :season: int of season, eg 16 is season 2016/17
    :league: league to query, tested: 1, 2
    
    :gameCSV: :tableCSV: csv files to store final data in
    
    output: "AllTables.csv"
    """
    
    # prepare/load outout DF
    collist = ['Retrieve_Date', 'Season', 'League', 'GameDay', 
               'rank', 'Team', 'sp', 'g', 'u', 'v', 'tore', 'diff', 'points']
    try:
        outDF = pd.read_csv(tableCSV, sep=";", encoding="utf8")
    except:
        outDF = pd.DataFrame(columns=collist)
    
    # check if specific season, league, gameday combination is already in list and skip if so
    if len(outDF[ (outDF['Season'] == season) & (outDF['League'] == league) & (outDF['GameDay'] == gameday)]) > 0:
        print("Season ", season, "League ", league, "Gameday ", gameday, "  skipped")
        return
    
    
    options = webdriver.firefox.options.Options()
    options.add_argument('-headless')
    driver = webdriver.Firefox(firefox_options=options)
    
    driver.get("http://www.kicker.de/news/fussball/bundesliga/spieltag/{}-bundesliga/20{}-{}/{}/0/spieltag.html".format(
                league, fix_season(season), fix_season(season+1), gameday)) 
    blankHTML = driver.page_source
    soup = BeautifulSoup(blankHTML, "lxml") 

    # find table element, return doing nothing if no data was found (eg gameday not played yet)
    table = soup.find("table", {"summary" : 'Tabelle', "class" : 'tStat'})
    
    # if no table exisits on the site, gameday has not been played
    if table is None:
        print("Gameday ", gameday, " doesn't exist, ending")
        return
    
    try:
        table_entries = table.find_all("td", {"class" : 'first'})
    except:
        return
    
    for rank, team in enumerate(table_entries):
        team_entries = [i for i in team.next_siblings]        
        
        try:
            sp = int(team_entries[7].text)
        except:
            sp = int(team_entries[6].text)
        
        # find index of first entry
        try:
            team_entries[10].text
            fe = 10
        except:
            fe = 11

        g = int(team_entries[fe].text)
        u = int(team_entries[fe+2].text)
        v = int(team_entries[fe+4].text)

        tore = team_entries[fe+8].text
        diff = int(team_entries[fe+10].text)
          
        points = int(team_entries[fe+14].text)
        
        team = team_entries[3].text.strip()
        for qualifier in ['(N)', '(M)', '(P)', '(A)']:
            if qualifier in team:
                team = team[:-4]
        if '(M, P)' in team:
                team = team[:-7]


        outDF = outDF.append({'Retrieve_Date' :  pendulum.now().to_date_string(),
                              'Season' : season, 
                              'League' : league, 
                              'GameDay' : gameday,
                              'rank' : rank+1, 
                              'Team' : team, 
                              'sp' : sp, 
                              'g' : g, 'u' : u, 'v' : v, 
                              'tore' : tore, 
                              'diff' : diff, 
                              'points' : points}, 
                              ignore_index=True)

        
    # cut off not needed columns (drops not needed index columns)
    outDF = outDF[collist]    
    # drop duplicates in case of double fetched data - dont use GoalsList col, as lists are not supported for drop_duplicates
    outDF.drop_duplicates(subset=collist[:-1], inplace=True)
    
    outDF.to_csv(tableCSV, sep=";")
    
    print("Season ", season, "League ", league, "Gameday ", gameday, "  done")
    
    driver.close()



def getAllTeamPages(inCsvFile):
    """
    Get a list and current kicker urls of all teams in league 1 and 2 since 2004
    
    output: "AllTeamPages.csv"
    """
    
    # get curren season
    cur_season = getCurrentSeason()       
    
    # prepare/load outout DF
    collist = ['Retrieve_Date', 'Season', 'League', 'Team', 'url_Teamname', 'Team_URL']
    try:
        outDF = pd.read_csv(inCsvFile, sep=";", encoding="utf8")
    except:
        outDF = pd.DataFrame(columns=collist)
    
    
    options = webdriver.firefox.options.Options()
    options.add_argument('-headless')
    driver = webdriver.Firefox(firefox_options=options)
    
    for league in range(1,3):
        for season in range(4, cur_season+1):
                
            # check if specific season, league, gameday combination is already in list and skip if so
            if len(outDF[ (outDF['Season'] == season) & (outDF['League'] == league)]) > 0:
                print("Season ", season, "League ", league, "  skipped")
                continue
            
    
            driver.get("http://www.kicker.de/news/fussball/bundesliga/spieltag/{}-bundesliga/20{}-{}/{}/0/spieltag.html".format(
                       league, fix_season(season), fix_season(season+1), 1)) 
            blankHTML = driver.page_source
            soup = BeautifulSoup(blankHTML, "lxml") 
            
            # find table element, return doing nothing if no data was found (eg gameday not played yet)
            table = soup.find("table", {"summary" : 'Tabelle', "class" : 'tStat'})
            try:
                table_entries = table.find_all("td", {"class" : 'first'})
            except:
                return
            
            for team in table_entries:
                team_entries = [i for i in team.next_siblings]  
                
                alltag = team_entries[3].find("div", {"id" : re.compile("ctl00_PlaceHolderContent_tabelle_ctl\d\d_repTabelle_ctl\d\d_ctl\d\d_verlinkt")})
                                                             
                specURL = alltag.next.next.next.next.next['href']

                url = "http://www.kicker.de" + specURL
                
                urlTeamName = specURL.split("/")[-2]
                
                team = team_entries[3].text.strip()
                for qualifier in ['(N)', '(M)', '(P)', '(A)']:
                    if qualifier in team:
                        team = team[:-4]
                if '(M, P)' in team:
                        team = team[:-7]
                        
                
            
                outDF = outDF.append({'Retrieve_Date' :  pendulum.now().to_date_string(),
                                      'Season' : season, 
                                      'League' : league, 
                                      'Team' : team,
                                      'url_Teamname' : urlTeamName,
                                      'Team_URL' : url}, 
                                      ignore_index=True)

    
                print("Season ", season, "League ", league, "Team ", team, "  done")
                
            # cut off not needed columns (drops not needed index columns)
            outDF = outDF[collist]    
            # drop duplicates in case of double fetched data - dont use GoalsList col, as lists are not supported for drop_duplicates
            outDF.drop_duplicates(subset=collist[:-1], inplace=True)
            
            outDF.to_csv(inCsvFile, sep=";") 
    
    driver.close()


def getScore(entry):
    """
    extracts final score from "cryptic" kicker results 
    """
    try:
        left, right = entry.split("\xa0")[0], entry.split("\xa0")[1]       
    
        if right[-1] == ")":
            return left
        else:
            return right
        
    except:
        return "0:0" # use this as default in few cases with cryptic text instead of results

def getOvertime(entry):
    """
    returns 1 if game ended with n.V. or i.E. flag 
    """
    left = entry.split("\xa0")[0]
    
    if left[0] == "n" or left[0] == "i":
        return 1
    else:
        return 0


def getTeamData(inCsvFile, mode = 'u', rec_url = None):
    """
    Get ALL games of a team by season, go back until season 2004
    
    :inCsvFile: file with exisiting data
    :mode: 'u' (default) will only update missing data from current season
           'a' will fetch All data back until 2004 (should only use for initial build of file)
    :rec_url: is the most recent url, data from this will be used to update missing or go back to 2004 from
    """
    
    # get curren season
    cur_season = getCurrentSeason()  
    
    # prepare/load outout DF
    collist = ['Retrieve_Date', 'Team', 'Season', 'Gegner', 'Wettbewerb', 'Spt./Runde', 'Termin', 'Wo', 'Score',
       'Overtime']
    try:
        outDF = pd.read_csv(inCsvFile, sep=";", encoding="utf8")
    except:
        outDF = pd.DataFrame(columns=collist)
    
    options = webdriver.firefox.options.Options()
    options.add_argument('-headless')
    driver = webdriver.Firefox(firefox_options=options)

    #recent_url = "http://www.kicker.de/news/fussball/bundesliga/vereine/1-bundesliga/2018-19/borussia-dortmund-17/vereinstermine.html"
    # extract recent season from rec_url
    rec_season = rec_url.split('/')[8][2:4]
    
    if mode == 'a':
        
        # work backwards through seasons
        for i in range(int(rec_season), 3, -1):
                        
            # rebuild rec_url for each iteration with correct season
            split_url = rec_url.split('/')
            split_url[8] = "20" + str(fix_season(i)) + "-" + str(fix_season(i+1))
            url = "/".join(split_url)
            
            # skip season/team if already in list
            if  len(outDF[ (outDF['Season'] == i) & (outDF['Team'] == split_url[-2])]) > 0:
                print("skipping ",  split_url[-2], " season ", i)
                continue
            
            # try reloading after timeout
            try:
                driver.get(url) 
            except:
                driver.get(url)
            blankHTML = driver.page_source
            soup = BeautifulSoup(blankHTML, "lxml") 
            
            # ocate table in html code, read html table with pandas
            table = soup.find("table", {"class" : "tStat", "summary" : "Tabelle"})
            pdTable = pd.read_html(str(table), header=0)[0]
            
            # clean up table
            pdTable = pdTable.iloc[ : , 0:6]
            try:
                pdTable.dropna(subset=["Gegner"], inplace=True)
            except:
                continue
            
            # ffill missing Wettbewerb data
            pdTable = pdTable.fillna(method='ffill')
            
            # get a clean column with final result
            pdTable["Score"] = pdTable.apply(lambda row: getScore(row['i']), axis=1)
            
            # get a column indicating if game was n.V. or i.E.
            pdTable["Overtime"] = pdTable.apply(lambda row: getOvertime(row['i']), axis=1)
            
            # delete obsolete orignal results column, labeled i 
            pdTable.drop(["i"], axis=1, inplace=True)
            
            # retrieval date column added
            pdTable['Retrieve_Date'] = pendulum.now().to_date_string()
            pdTable['Team'] = split_url[-2]
            pdTable['Season'] = i
            
            pdTable.rename(columns={"Ergebnis":"Wo"})
            
            # append table
            outDF = outDF.append(pdTable, ignore_index=True)
            
            # drop duplicates, determined by date, when in doubt keep later crawled
            outDF.drop_duplicates(subset=['Termin'], keep='last', inplace=True)
            
            outDF = outDF[collist]
            
            outDF.to_csv(inCsvFile, sep=";") 
            print("Season ", i, " Team ", split_url[-2], " done")
    
    driver.close()



def teamResultsBuilder(teamListcsv, outCsv="AllTeamResults.csv"):
    """
    Build a teamresults.csv using getTeamData() for all teams from getAllTeamPages() output
    
    output: "AllTeamResults.csv"
    """
    
    teamList = pd.read_csv(teamListcsv, sep=";")
    
    # sort so latest season is on top, drop duplicate entries keep first
    teamList.sort_values("Season", ascending=False, inplace=True)
    single_list = teamList.drop_duplicates("Team")
    
    for url in single_list["Team_URL"]:
        # urls must end in vereinstermine.html, currently ending in vereinsinformationen.html
        url_split = url.split("/")
        url_split[-1] =  "vereinstermine.html"
        url = "/".join(url_split)
        
        
        getTeamData(outCsv, mode = 'a', rec_url = url)
        
    





