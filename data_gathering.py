# -*- coding: utf-8 -*-

"""
This file handles the data download and updating process, for BL 1 & 2 data back until 2004

If starting from scratch:
    1. Run getTableFromKicker() - this will download the tables for each gameday 
                                    (run in 3 loops for season, league and gameday)
                                    Does not need anything else run beforehand
    
    2. Run getAllTeamPages() - returns a list of all teams in a season and league incl. reference URL
                                Will automatically loop back to 2004, no outer loops needed
    
    3. Run teamResultsBuilder() -  this will use data from step and pass the URLs of each team
                                   to getTeamResults(), which downloads all results of the team per season
                                   for all teams found in original table
                                   
                                   Run in mode "a" for all

If updating exsiting files:
    1. Just run updateAll() - this will update all passed files for the current season
                                Will actually fetch data for current season again

"""

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


def getTableFromKicker(season, league, gameday, tableCSV, force = False):
    """
    Build a DF for each GameDay's table
    
    :season: int of season, eg 16 is season 2016/17
    :league: league to query, tested: 1, 2
    
    :gameCSV: :tableCSV: csv files to store final data in
    
    :output: "AllTables.csv"
    
    :force: reload table even if it already exists
    """
    
    # prepare/load outout DF
    collist = ['Retrieve_Date', 'Season', 'League', 'GameDay', 
               'rank', 'Team', 'sp', 'g', 'u', 'v', 'tore', 'diff', 'points']
    try:
        outDF = pd.read_csv(tableCSV, sep=";", encoding="utf8")
    except:
        outDF = pd.DataFrame(columns=collist)
    
    # check if specific season, league, gameday combination is already in list and skip if so
    if force == False:
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
        if ' *' in team:
                team = team[:-2]


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


def getTeamResults(inCsvFile, mode = 'u', rec_url = None):
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
    
    driver.set_page_load_timeout(60)

    #recent_url = "http://www.kicker.de/news/fussball/bundesliga/vereine/1-bundesliga/2018-19/borussia-dortmund-17/vereinstermine.html"
    # extract recent season from rec_url
    rec_season = rec_url.split('/')[8][2:4]
    
    if mode == 'a':
        
        # work backwards through seasons, until 2004
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
            
            pdTable = pdTable.rename(columns={"Ergebnis":"Wo"})
            
            # append table
            outDF = outDF.append(pdTable, ignore_index=True)
            
            # drop duplicates, determined by date, when in doubt keep later crawled
            outDF.drop_duplicates(subset=['Termin', 'Team'], keep='last', inplace=True)
            
            outDF = outDF[collist]
            
            outDF.to_csv(inCsvFile, sep=";") 
            print("Season ", i, " Team ", split_url[-2], " done")
    
    
    elif mode == 'u':
        split_url = rec_url.split('/')
        url = rec_url
        team = split_url[-2]
        
        
        # remove all data from current season and team
        outDF = outDF[ ~((outDF["Season"] == cur_season) & (outDF["Team"] == team)) ] 
                
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
            return
        
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
        pdTable['Season'] = cur_season
        
        pdTable = pdTable.rename(columns={"Ergebnis":"Wo"})
        
        # append table
        outDF = outDF.append(pdTable, ignore_index=True)
        
        # drop duplicates, determined by date, when in doubt keep later crawled
        outDF.drop_duplicates(subset=['Termin', 'Team'], keep='last', inplace=True)
        
        outDF = outDF[collist]
        
        outDF.to_csv(inCsvFile, sep=";") 
        print("Season ", cur_season, " Team ", split_url[-2], " updated")

    
    driver.close()



def teamResultsBuilder(teamListcsv="AllTeamPages.csv", mode = 'u', outCsv="AllTeamResults.csv"):
    """
    Build a teamresults.csv using getTeamResults() for all teams from getAllTeamPages() output
    
    output: "AllTeamResults.csv"
    """
    
    teamList = pd.read_csv(teamListcsv, sep=";")
    
    cur_season = getCurrentSeason() 
    
    if mode == 'a':
        # sort so latest season is on top, drop duplicate entries keep first
        teamList.sort_values("Season", ascending=False, inplace=True)
        single_list = teamList.drop_duplicates("Team")
        
        for url in single_list["Team_URL"]:
            # urls must end in vereinstermine.html, currently ending in vereinsinformationen.html
            url_split = url.split("/")
            url_split[-1] =  "vereinstermine.html"
            url = "/".join(url_split)
            
            
            getTeamResults(outCsv, mode = 'a', rec_url = url)
    
    elif mode == 'u':
        # will effectivly delete present entry for club and season, fetch it again and append new data
        teamList_current = teamList[teamList['Season'] == cur_season]
    
        for url in teamList_current["Team_URL"]:
            # urls must end in vereinstermine.html, currently ending in vereinsinformationen.html
            url_split = url.split("/")
            url_split[-1] =  "vereinstermine.html"
            url = "/".join(url_split)
            
            
            getTeamResults(outCsv, mode = 'u', rec_url = url)



def getCurrentList(in_urls):
    """
    If url is not from current season, creates iteratively possible urls for lower leagues 
    """
    cur_season = getCurrentSeason() 
    
    out_list = []
    
    for url in in_urls:
        url_split = url.split("/")
        
        # if url already correct, simply append
        if url_split[-3] == "20" + fix_season(cur_season) + "-" + fix_season(cur_season+1):
            out_list.append(url)
        
        else:
            # replace date part of string with current season
            cur_season_str = "20" + fix_season(cur_season) + "-" + fix_season(cur_season+1)
            url_split[-3] = cur_season_str
            
            for l in ["bundesliga", "2bundesliga", "3-liga"]:
                url_split[-4] = l
                out_url = "/".join(url_split)
                out_list.append(out_url)
    
    return out_list
        


def getCoaches(teamListcsv="AllTeamPages.csv", mode = "u", outCsv = "AllTeamCoaches.csv"):
    """
    Get all the coaches of a team
    Uses most recent urls from teamListcsv to go through coaches list    
    
    :mode: if "u" only currently BL1 and BL2 coaches are updated (site fetching with most recent url
                                                                only works for current season links)
              "a" all coaches are fetched, this season most recent url for each team must be determined
                                                                first
    """
    
    collist = ['Retrieve_Date', 'Team', "Vorname", "Nachname", "Geboren", "Nationalität",
               "von", "bis"]
    try:
        outDF = pd.read_csv(outCsv, sep=";", encoding="utf8")
    except:
        outDF = pd.DataFrame(columns=collist)
    
    options = webdriver.firefox.options.Options()
    options.add_argument('-headless')
    driver = webdriver.Firefox(firefox_options=options)
    
    teamList = pd.read_csv(teamListcsv, sep=";")
    teamList.sort_values("Season", ascending=False, inplace=True)
    single_list = teamList.drop_duplicates("Team")
    
    # only fetch teams that currently are in league 1 or 2
    single_list = single_list[single_list["Season"] == single_list["Season"].max()] 
    
    if mode == "u":    
        url_list = single_list["Team_URL"]
    
    if mode == "a":
        url_list  = getCurrentList(single_list["Team_URL"])
    
    for url in url_list:
            # urls must end in vereinstermine.html, currently ending in vereinsinformationen.html
            url_split = url.split("/")
            url_split[-1] =  "trainer.html"
            url = "/".join(url_split)
            
            try:
                driver.get(url)
            except:
                driver.get(url)
            
            blankHTML = driver.page_source
            soup = BeautifulSoup(blankHTML, "lxml") 
            
            # find main container with all coaches in it
            main_container = soup.find("div", {"id" : "slidercontainer"})
            
            try:
                # get one container per coach
                coaches = main_container.find_all("div", {"class" : "trainerverlauf_Container"})
            except:
                print("Skipped emtpy ", url)
                continue
            
            # get first part with coach general data
            for c in coaches:
                coach_cv = c.find_all("tbody")[0]
                coach_cv_subs = coach_cv.find_all("tr")
                
                for entry in coach_cv_subs:
                    if 'Vorname' in entry.text:
                        vorname = entry.text[entry.text.find(":") + 1: ]
                    if 'Nachname' in entry.text:
                        nachname = entry.text[entry.text.find(":") + 1: ]
                    if 'Geboren' in entry.text:
                        geboren = entry.text[entry.text.find(":") + 1: ].strip()
                    if 'Nation' in entry.text:
                        nation = entry.text[entry.text.find(":") + 1: ]
                    if 'von:' in entry.text:
                        von = entry.text[entry.text.find(":") + 1 : entry.text.rfind(",")]
                    if 'bis:' in entry.text:
                        if "," in entry.text:
                            bis = entry.text[entry.text.find(":") + 1 : entry.text.rfind(",")]
                        else:
                            bis = entry.text[entry.text.find(":") + 1 : ]
                    
                outDF = outDF.append({'Retrieve_Date' :  pendulum.now().to_date_string(),
                                      'Team' : url_split[-2], 
                                      "Vorname" : vorname, 
                                      "Nachname" : nachname, 
                                      "Geboren" : geboren, 
                                      "Nationalität" : nation,
                                      "von" : von, 
                                      "bis" : bis}, 
                                    ignore_index=True)
                
                outDF.drop_duplicates(subset=['Team', 'Vorname', 'Nachname', 'von'] ,inplace=True)
                
            print(len(coaches), " Coaches of ", url_split[-2] ,"done")    
    
            outDF.to_csv(outCsv, sep=";")



def getCurrentGameDay(league, in_df):
    """
    Returns the current GameDay (int) as determined from  :in_df:
        for :league: 1 or 2
        
        :in_df: should only contain data from current season
    """ 
    
    # reduce entries to only contain BL or 2.BL entries
    if league == 1:
        in_df = in_df[in_df["Wettbewerb"].isin(["BL"])]
    elif league == 2:
        in_df = in_df[in_df["Wettbewerb"].isin(["2.BL"])]
    
    # choose first team from current season
    in_df = in_df[in_df["Team"] == in_df["Team"].unique()[0] ]
    # remove all scores that are -:-
    in_df = in_df[in_df["Score"] != "-:-" ]

    # sort by gameday, recent / highest number is on top
    in_df = in_df.sort_values("Spt./Runde", ascending=False)
    # get gameday by index locations
    last_gameday = in_df.iloc[0,6]
    
    # clean last_gameday, as it is still a string in form 9. Spt. or 10. Spt.
    return int(last_gameday[ : last_gameday.find(".")])



def updateAll(allTeamPages = "AllTeamPages.csv", allTeamResults = "AllTeamResults.csv", 
              allTables = "AllTables.csv", allCoaches = "AllTeamCoaches.csv"):
    """
    will update the above specified files for current season & gameday
    
    will update allTeamResults and allTables even if they are up to date! (Refetch everything for cur season)
    
    Should not be run on actual gamedays but only AFTER, unwanted behaviour expected otherwise
    """
    
    cur_season = getCurrentSeason() 
    
    # will check if AllTeamPages was run for current season. If run, will be skipped
    aTP_df = pd.read_csv(allTeamPages, sep=";")
    if cur_season not in aTP_df["Season"].unique():
        getAllTeamPages(allTeamPages)
        print("Teampages updated")
    else:
        print("Teampages not updated, still up-to-date")
        
    
    # # # # # # 
    
        
    # update AllTeamResults next, to use this to extract current gameday
    teamResultsBuilder(allTeamPages, mode = 'u', outCsv=allTeamResults)
    print("Teamresults updated")
    
    
    # # # # # # 
    
    
    # use first team in list for current season and extract last played gameday
    aTR_df = pd.read_csv(allTeamResults, sep=";")
    # reduce to current season
    aTR_df_cur = aTR_df[ aTR_df["Season"] == cur_season ]
    
    # for current approach redundant
    #last_gameday_BL1 = getCurrentGameDay(1, aTR_df_cur)
    #last_gameday_BL2 = getCurrentGameDay(2, aTR_df_cur)
    
    
    # update AllTables, by checking how many gamedays are missing 
    aT_df = pd.read_csv(allTables, sep=";")
    data_game_day_BL1 = aT_df[ (aT_df["Season"]==cur_season) & (aT_df["League"]==1)]["GameDay"].max()
    data_game_day_BL2 = aT_df[ (aT_df["Season"]==cur_season) & (aT_df["League"]==2)]["GameDay"].max()
    
    # cut off current season, save file back to csv, then re-run current season
    aT_df  = aT_df [aT_df ["Season"] != cur_season]
    aT_df.to_csv(allTables, sep=";")
    
    for l in [1,2]:
        
        # determine maximum gameday to crawl in current season
        if l == 1:
            upper_boundary = data_game_day_BL1
        else:
            upper_boundary = data_game_day_BL2
        
        # ensure maximum gameday is 34
        upper_boundary = min(upper_boundary, 34)
        
        for g in range(1, upper_boundary+2):
            getTableFromKicker(cur_season, l, g, allTables)

    print("TeamTables updated")


    # # # # #
    getCoaches(teamListcsv=allTeamPages, mode='u', outCsv = allCoaches)
    




#updateAll(allTeamPages = "E:/Test/AllTeamPages.csv", allTeamResults = "E:/Test/AllTeamResults.csv", 
#              allTables = "E:/Test/AllTables.csv", allCoaches = "E:/Test/AllTeamCoaches.csv")












