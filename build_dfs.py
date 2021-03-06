# -*- coding: utf-8 -*-

# builds one comprehensive dataframe from data_gathering.py data

import numpy as np
import pandas as pd
import json
import warnings


import pendulum

import data_gathering

# disable warnings from pandas
warnings.filterwarnings('ignore')

# # # # # # # # # LOAD RAW DATA # # # # # # # # #

data_folder = "D:/Stuff/Projects/kicktipp/"

allTeamPages = pd.read_csv(data_folder + "AllTeamPages.csv", sep=";")
allTeamResults = pd.read_csv(data_folder + "AllTeamResults.csv", sep=";")
allTables = pd.read_csv(data_folder + "AllTables.csv", sep=";")
allCoaches = pd.read_csv(data_folder + "AllTeamCoaches.csv", sep=";")

with open("C:/WorkExchange/Python/Git/kt/alias.json", "r", encoding="utf8") as j:
    alias_json = json.load( j )


# # # # # # Input Processing # # # # # # # # # # #

# Dynamo Dresden exsits with 2 different plain names in table; other writing errors fixed as well
allTables.replace({'1. FC Dynamo Dresden' : 'Dynamo Dresden',
                   "LR Ahlen" : 'Rot Weiss Ahlen',
                   'Arminia Bielefeld (' : 'Arminia Bielefeld'}, inplace = True)

    
for col in ["von", "bis"]:
    allCoaches[col] = pd.to_datetime(allCoaches[col], errors="coerce", format="%d.%m.%Y")

# # # # # # # # # PANDAS OPTIONS # # # # # # # # #
pd.set_option('display.max_columns', 100)


# # # # # # # # # HELPER FUNCTIONS # # # # # # # # #

def translateTeam(inTeam):
    """
    Return key of alias_json for passed team, returns itself if nothing is found
    """    
   
    # list should contain exactly one entry if inTeam exists as a dictionary value
    if len( [ key_values for key_values in alias_json.items() if inTeam in key_values[1] ] ) == 1:       
        return [ key_values for key_values in alias_json.items() if inTeam in key_values[1] ][0][0]
    
    else:
        return inTeam


def getKickerTeamName(inTeam):
    """
    returns the team string used in kicker urls
    """    
    return [ x for x in alias_json[translateTeam(inTeam)] if "-" in x ][0]


def seasonFromDate(inDate):
    """
    Returns season (int) that passed date lies in. August splits season (August 2018 is part of season 18/19)
    
    :inDate: pendulum datetime object
    """    
    if inDate.month < 8:
        cur_season = inDate.year - 2001
    else:
        cur_season = inDate.year - 2000
    return cur_season


def getPastLeagues(team, start_season):
    """
    returns the number of leagues team has played in, in last 5 seasons
    Will return 3 for each season team was not in 1 or 2 (regardless of actual league)
    """
    outList = []
    for i in range(1,6):
        # will fail if team wasnt in league 1 or 2
        try:            
            l = allTables[ (allTables["Team"] == team) & (allTables["Season"] == start_season-i) & (allTables["GameDay"] == 34) 
                          ]["League"].values[0]
        except:
            l = 3
        outList.append(l)
    return outList




def switch_teams(df):
    """
    Switches Team1 and Team2 in dataframe, so hometeam is always Team1
    """
    # get a new row with my own index, use this to rebuild df later in exact same order
    df["my_idx"] =  range(len(df))
    
    is_okay_df = df[df['Team1_Home'] == 1]
    switch_df =  df[df['Team1_Home'] == 0]
    
    switch_df = switch_df.rename(columns={'Team1' : 'Team2', 'Team2' : 'Team1',
                                          'GamesSinceLastWin1' : 'GamesSinceLastWin2', 'GamesSinceLastWin2' : 'GamesSinceLastWin1',
                                          'TimeSinceLastGame1' : 'TimeSinceLastGame2', 'TimeSinceLastGame2' : 'TimeSinceLastGame1', 
                                          'LastGameOverTime1' : 'LastGameOverTime2', 'LastGameOverTime2' : 'LastGameOverTime1',
                                          'TimeSinceLastCoach1' : 'TimeSinceLastCoach2', 'TimeSinceLastCoach2' : 'TimeSinceLastCoach1',
                                          'CurrentPoints1' : 'CurrentPoints2', 'CurrentPoints2' : 'CurrentPoints1',
                                          'CurrentPosition1' : 'CurrentPosition2', 'CurrentPosition2' : 'CurrentPosition1',
                                          'CurrentGoalDif1' : 'CurrentGoalDif2', 'CurrentGoalDif2' : 'CurrentGoalDif1',
                                          'CurrentWin1' : 'CurrentWin2', 'CurrentWin2' : 'CurrentWin1', 
                                          'CurrentDraws1' : 'CurrentDraws2','CurrentDraws2' : 'CurrentDraws1',
                                          'CurrentLoss1' : 'CurrentLoss2','CurrentLoss2' : 'CurrentLoss1',
                                          'LastSeasonPosition1' : 'LastSeasonPosition2', 'LastSeasonPosition2' : 'LastSeasonPosition1',
                                          'LastSeasonLeague1' : 'LastSeasonLeague2', 'LastSeasonLeague2' : 'LastSeasonLeague1', 
                                          'Past5YearsInThisLeague1' : 'Past5YearsInThisLeague2', 'Past5YearsInThisLeague2' : 'Past5YearsInThisLeague1',
                                          'LastGameTeam1_1' : 'LastGameTeam2_1', 'LastGameTeam2_1' : 'LastGameTeam1_1',
                                          'LastGameTeam1_2' : 'LastGameTeam2_2','LastGameTeam2_2' : 'LastGameTeam1_2',
                                          'LastGameTeam1_3' : 'LastGameTeam2_3', 'LastGameTeam2_3' : 'LastGameTeam1_3',
                                          'LastGameTeam1_4' : 'LastGameTeam2_4', 'LastGameTeam2_4' : 'LastGameTeam1_4',
                                          'LastGameTeam1_5' : 'LastGameTeam2_5','LastGameTeam2_5' : 'LastGameTeam1_5',
                                          'CL_candidate1' : 'CL_candidate2', 'CL_candidate2' : 'CL_candidate1',
                                          'EL_candidate1' : 'EL_candidate2', 'EL_candidate2' : 'EL_candidate1'})
     
    # last direct game results must be switched as well
    man_switch = ['Result','LastDirectGame1', 'LastDirectGame2', 'LastDirectGame3']
    for col in man_switch:
        # try switching results, if "Result" column not present, continue
        try:
            switch_df[col] = switch_df.apply(lambda row: row[col][::-1], axis=1)
        except:
            pass
        
    switch_df = switch_df[is_okay_df.columns]
    is_okay_df = is_okay_df.append(switch_df)
    
    is_okay_df = is_okay_df.sort_values("my_idx")
    is_okay_df.drop(["my_idx", "Team1_Home", "Team2_Home"], axis=1, inplace=True)
    
    return is_okay_df


# # # # # # # # # BUILD INPUT DF # # # # # # # # #

def createHumanFrame(allTeamResults=allTeamResults, allTables=allTables, allCoaches=allCoaches, outFile="human_table.csv"):
    """
    Use basic data (data_gathering.py output) to create comprehensive DataFrame 
    for actual modelling
    
    :allX: input DataFrames from data_gathering.py
    :outFile: will store the data, if already contains games, these will be skipped in consecutive runs
    """
    
    print("Creating Human Frame")
    
    # try loading output file or create new one if path is given
    try:
        outDF = pd.read_csv(outFile, sep=";", encoding="utf8")
    except:
        outDF = pd.DataFrame(columns=['Retrieve_Date',
                                  'Game_Date',
                                  
                                 "Team1", 
                                 "Team2", 
                                 
                                 "CurLeague", # Current League
                                 "Result", # Game Endresult
                                 
                                 "Team1_Home", # 1 if Team 1 is Hometeam, else 0
                                 "Team2_Home", # 1 if Team 2 is Hometeam, else 0
                                 
                                 "GameTimeOfDay", # Time of Game
                                 "GameWeekday", # Weekday of Game
                                 "GameDay", # GameDay in League
                                 
                                 "GamesSinceLastWin1", # No. of games since last won game Team1
                                 "GamesSinceLastWin2", # No. of games since last won game Team2
                                 
                                 "TimeSinceLastGame1", # Time in Hours since last game Team 1
                                 "TimeSinceLastGame2", # Time in Hours since last game Team 2
                                 
                                 "LastGameOverTime1", # Was last game of Team 1 with overtime or penalty shootout
                                 "LastGameOverTime2", # Was last game of Team 2 with overtime or penalty shootout
                                 
                                 "TimeSinceLastCoach1", # Time since Team 1 has current coach (if any)
                                 "TimeSinceLastCoach2", # Time since Team 2 has current coach (if any)
                                 
                                 "CurrentPoints1", # current position in league Team 1
                                 "CurrentPoints2", # current position in league Team 2
                                 
                                 "CurrentPosition1", # current position in league Team 1
                                 "CurrentPosition2", # current position in league Team 2
                                 
                                 "CurrentGoalDif1", # current goal difference Team 1
                                 "CurrentGoalDif2", # current goal difference Team 2
                                 
                                 "CurrentWin1", # current wins in season of Team 1
                                 "CurrentDraws1", # current draws in season of Team 1
                                 "CurrentLoss1", # current losses in season of Team 1
                                 "CurrentWin2", # current wins in season of Team 2
                                 "CurrentDraws2", # current draws in season of Team 2
                                 "CurrentLoss2", # current losses in season of Team 2
                                 
                                 "LastSeasonPosition1", # last season's final position in league Team 1
                                 "LastSeasonPosition2", # last season's final position in league Team 2
                                 
                                 "LastSeasonLeague1", # last season league of Team 1
                                 "LastSeasonLeague2", # last season league of Team 2
                                 
                                 "Past5YearsInThisLeague1", # 1 if Team1 played in same league for past 5 years, else 0
                                 "Past5YearsInThisLeague2", # 1 if Team2 played in same league for past 5 years, else 0
                                 
                                 "LastDirectGame1", # Last direct meeting of both teams results (0:0 if none)
                                 "LastDirectGame2", # 2nd last direct meeting of both teams results (0:0 if none)
                                 "LastDirectGame3", # 3rd last direct meeting of both teams results (0:0 if none)
                                 
                                 "LastDirectGame1_time", # Time in days since last direct meeting of both teams results (99999 if none)
                                 "LastDirectGame2_time", # Time in days since 2nd last direct meeting of both teams results (99999 if none)
                                 "LastDirectGame3_time", # Time in days since 3rd last direct meeting of both teams results (99999 if none)
                                 
                                 "LastGameTeam1_1", # Last 5 game results of Team 1                                 
                                 "LastGameTeam1_2",
                                 "LastGameTeam1_3",
                                 "LastGameTeam1_4",
                                 "LastGameTeam1_5",
                                 "LastGameTeam2_1", # Last 5 game results of Team 2
                                 "LastGameTeam2_2",
                                 "LastGameTeam2_3",
                                 "LastGameTeam2_4",
                                 "LastGameTeam2_5",
                                 
                                 "CL_candidate1", # Team 1 playing Champions League this season
                                 "CL_candidate2", # Team 2 playing Champions League this season
                                 
                                 "EL_candidate1", # Team 1 playing Europe League this season
                                 "EL_candidate2", # Team 2 playing Europe League this season
                                 ])
    
    
    
    # # # general DF column adding etc. # # # 
        
    # split x:x into two columns T1Goals for Hometeam Goals and T2Goals for away team goals
    allTeamResults = allTeamResults.join(allTeamResults["Score"].str.split(":", expand=True)
                        ).rename(columns={0:"T1Goals", 1:"T2Goals"})
    
    allTeamResults = allTeamResults.replace('-', np.nan)
    
    # calculate field with goal difference
    allTeamResults["Intmd"] = allTeamResults["T1Goals"].astype(float, errors='ignore').subtract(allTeamResults["T2Goals"].astype(float, errors='ignore'))
    # divide by its self absolute value ->  team1 win == 1, draw == 0, loss = -1
    allTeamResults["IsWin"] = allTeamResults["Intmd"].divide( allTeamResults["Intmd"].abs())
    # replace division by 0 values with 0, but only for existing results
    allTeamResults["IsWin"] = allTeamResults[allTeamResults["Score"] != "-:-"]["IsWin"].fillna(0)
    
    
    # convert Termin column to new DateTime type column Date
    allTeamResults["Date"] = pd.to_datetime(allTeamResults["Termin"].str.slice(4), errors='coerce', format='%d.%m.%y %H:%M')
    
    
    
    # list will hold tuples of games, if same game is found in another teams list, its not put into df again
    skip_list = [] 
    
    # iterate over allTeamResults and extract infos for each game
    for row_tup in allTeamResults.iterrows():
        row_index = row_tup[0]
        row = row_tup[1] # original row returns a tuple with first elem as index, second elem as data
        
        print('\r', '{} / {}  '.format(row_index, len(allTeamResults)), end="")
    
        # skip adding if game was not in 1st or 2nd BL
        if row["Wettbewerb"] not in ['BL', '2.BL']:
            continue
        
        # skip adding if game has not been played yet
        if row["Score"] == '-:-':
            continue
        
        pendulum_time = pendulum.from_format(row["Termin"][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin')  
        date_season = seasonFromDate(pendulum_time)
        
        # skip if game is before 2005, as one seaosn before is needed for data gathering
        if date_season < 5:
            continue
        
        
        # # # # Check things same for both teams # # # # # 
        
        team1 = translateTeam( row["Team"] )
        team2 = translateTeam( row["Gegner"] )
        
        # determine numeric league
        if row["Wettbewerb"] == 'BL':
            cur_league = 1
        else:
            cur_league = 2
        result = row["Score"] 
        
        # skip game if was already in list in loaded output csv
        if len( outDF[ (outDF["Team1"] == team1) & (outDF['Game_Date'] == pendulum_time.to_date_string())] ) > 0:
            continue
        
        # skip game if already in list (bc reverse order of teams used as input before), if not in list, append to list then continue
        if (team2, team1, row["Termin"]) in skip_list:
            continue        
        skip_list.append( (team1, team2, row["Termin"]) )
        
        
        # get home team 
        if row["Wo"] == "H":
            t1_home = 1
            t2_home = 0
        elif row["Wo"] == "A":
            t1_home = 0
            t2_home = 1
    
       
        # # # time-related caluclations # # # 
        
        
        # game time in minutes since midnight, e.g. 13:00h == 780
        gameTimeMinutes = pendulum_time.hour * 60 + pendulum_time.minute        
        
        # get gameDay, only works for BL gamedays
        gameDay = int(row["Spt./Runde"][ : row["Spt./Runde"].find(".")]) 
        
        
        # # # # # # # # OTHERS # # # # # # # # # 
        
        
        # get no of games since last win
        
        # get df with only current team and only games WITH result 
        lf_df1 = allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team1)) & (allTeamResults["Score"] != "-:-") ]
        lf_df2 = allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team2)) & (allTeamResults["Score"] != "-:-") ]
        # sort lf_df by date, so last games are on bottom
        lf_df1 = lf_df1.sort_values("Date")        
        lf_df2 = lf_df2.sort_values("Date") 
        # cut_off all games after current
        lf_df1_cur = lf_df1[lf_df1["Date"] < pendulum_time.to_datetime_string()]  
        lf_df2_cur = lf_df2[lf_df2["Date"] < pendulum_time.to_datetime_string()] 
        # get last game with win (0 if last game was win) - reverse IsWin column as list and return index of first element that is 1
        last_game_won1 = lf_df1_cur["IsWin"].tolist()[::-1].index(1)
        last_game_won2 = lf_df2_cur["IsWin"].tolist()[::-1].index(1)
        
        
        # get time since last game in hours       
        lf_df1_reidx = lf_df1.reset_index()
        lf_df2_reidx = lf_df2.reset_index()
         # get index of row above current row
        t1_idx = lf_df1_reidx[lf_df1_reidx["Termin"] == row["Termin"]].index - 1
        t2_idx = lf_df2_reidx[lf_df2_reidx["Termin"] == row["Termin"]].index - 1 
        # get game time with index above
        try:
            last_game_time1 = pendulum.from_format(lf_df1_reidx.iloc[t1_idx]["Termin"].values[0][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin') 
        except:
            continue
        try:
            last_game_time2 = pendulum.from_format(lf_df2_reidx.iloc[t2_idx]["Termin"].values[0][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin')  
        except:
            continue
        # get difference to current game in hours
        t_diff1 = (pendulum_time - last_game_time1).in_hours()
        t_diff2 = (pendulum_time - last_game_time2).in_hours()
        
        # was last game overtime 
        t1_overtime = lf_df1_reidx.iloc[t1_idx]["Overtime"].values[0]
        t2_overtime = lf_df2_reidx.iloc[t2_idx]["Overtime"].values[0]
        
        
        # time since last coach
        t1_coaches = allCoaches[(allCoaches["Team"] == getKickerTeamName(team1)) & (allCoaches["von"] < pendulum_time.to_date_string())
                               ].sort_values("von")
        t2_coaches = allCoaches[(allCoaches["Team"] == getKickerTeamName(team2)) & (allCoaches["von"] < pendulum_time.to_date_string())
                               ].sort_values("von")
        # time difference in days between game and last coach recruiting
        try:
            t1_coach_diff = (pendulum_time - pendulum.instance(t1_coaches.iloc[-1]["von"], tz='Europe/Berlin')).in_days()
        except:
            t1_coach_diff = 99999
        try:
            t2_coach_diff = (pendulum_time - pendulum.instance(t2_coaches.iloc[-1]["von"], tz='Europe/Berlin')).in_days()
        except:
            t2_coach_diff = 99999
        
        
        # Get last 5 games as list
        l5Games1 = []
        l5Games2 = []
        for g in range(1,6):
            t1_idx = lf_df1_reidx[lf_df1_reidx["Termin"] == row["Termin"]].index - g
            l5Games1.append(lf_df1_reidx.iloc[t1_idx]["Score"].values[0])
            t2_idx = lf_df2_reidx[lf_df2_reidx["Termin"] == row["Termin"]].index - g 
            l5Games2.append(lf_df2_reidx.iloc[t2_idx]["Score"].values[0])     
        
        
        # Get last 3 direct games between both teams (manually account for teams that havent met 3 times, set 0:0 default)
        last_direct_df = lf_df1_reidx[ (lf_df1_reidx["Gegner"] == team2) & (lf_df1_reidx["Date"] < pendulum_time.to_datetime_string())].sort_values("Date")
        
        if len(last_direct_df) == 0:
            last_direct_3 = "0:0"
            last_direct_3_time = 99999
            last_direct_2 = "0:0"
            last_direct_2_time = 99999
            last_direct_1 = "0:0"
            last_direct_1_time = 99999
            
        
        elif len(last_direct_df) == 1:
            last_direct_3 = "0:0"
            last_direct_3_time = 99999
            last_direct_2 = "0:0"
            last_direct_2_time = 99999
            
            last_direct_1 = last_direct_df.iloc[-1]["Score"]
            last_direct_1_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-1]["Date"])).in_days()
        
        elif len(last_direct_df) == 2:
            last_direct_3 = "0:0"
            last_direct_3_time = 99999
            
            last_direct_2 = last_direct_df.iloc[-2]["Score"]
            last_direct_2_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-2]["Date"])).in_days()
            
            last_direct_1 = last_direct_df.iloc[-1]["Score"]
            last_direct_1_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-1]["Date"])).in_days()
        
        else:
            last_direct_3 = last_direct_df.iloc[-3]["Score"]
            last_direct_3_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-3]["Date"])).in_days()
            
            last_direct_2 = last_direct_df.iloc[-2]["Score"]
            last_direct_2_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-2]["Date"])).in_days()
            
            last_direct_1 = last_direct_df.iloc[-1]["Score"]
            last_direct_1_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-1]["Date"])).in_days()
        
            
    
        
        
        # table entry for date
        if gameDay > 1:
            table_entry1 = allTables[ (allTables["Team"] == team1) & (allTables["Season"] == date_season) & (allTables["GameDay"] == gameDay-1) ]
            table_entry2 = allTables[ (allTables["Team"] == team2) & (allTables["Season"] == date_season) & (allTables["GameDay"] == gameDay-1) ]
            
        # create dummy df with all 0 data for first gameday
        else:
            table_entry1 = pd.DataFrame(data={'Season':date_season, 'League': cur_league, 'GameDay':gameDay, 'rank':0,
           'Team':team1, 'sp':0, 'g':0, 'u':0, 'v':0, 'tore':0, 'diff':0, 'points':0}, index=[0])
            table_entry2 = pd.DataFrame(data={'Season':date_season, 'League': cur_league, 'GameDay':gameDay, 'rank':0,
           'Team':team2, 'sp':0, 'g':0, 'u':0, 'v':0, 'tore':0, 'diff':0, 'points':0}, index=[0])
        
        # last seasons last gameday entry
        ls_table_entry1 = allTables[ (allTables["Team"] == team1) & (allTables["Season"] == date_season-1) & (allTables["GameDay"] == 34) ]
        ls_table_entry2 = allTables[ (allTables["Team"] == team2) & (allTables["Season"] == date_season-1) & (allTables["GameDay"] == 34) ]
        
        # last 5 seasons league, 1 if all same as current league, 0 if at least one season was different
        t1_last5, t2_last5 = 0,0
        try:
            if (len(set(getPastLeagues(team1, date_season))) == 1) & (getPastLeagues(team1, date_season)[0]==table_entry1["League"].values[0]):
                t1_last5 = 1
        except:
            pass
        try:
            if (len(set(getPastLeagues(team2, date_season))) == 1) & (getPastLeagues(team2, date_season)[0]==table_entry2["League"].values[0]):
                t2_last5 = 1
        except:
            pass
            
        # last season positions, if no table exists, default to 3   
        try:
            lsp1 = ls_table_entry1["rank"].values[0]
        except:
            lsp2 = 3
        try:
            lsp2 = ls_table_entry2["rank"].values[0]
        except:
            lsp2 = 3
        
        # last season league, if no table exists, default to 3   
        try:
            lsl1 = ls_table_entry1["League"].values[0]
        except:
            lsl2 = 3
        try:
            lsl2 = ls_table_entry2["League"].values[0]
        except:
            lsl2 = 3
            
        
        
        # get if playing in CL or EL in current season
        t1_cl, t2_cl, t1_el, t2_el = 0,0,0,0
        if len(allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team1)) 
                      & (allTeamResults["Season"] == date_season)
                      & (allTeamResults["Wettbewerb"] == "CL")  ] ) > 0:
                t1_cl = 1
        if len(allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team2)) 
                      & (allTeamResults["Season"] == date_season)
                      & (allTeamResults["Wettbewerb"] == "CL")  ] ) > 0:
                t2_cl = 1
        if len(allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team1)) 
                      & (allTeamResults["Season"] == date_season)
                      & (allTeamResults["Wettbewerb"] == "EL")  ] ) > 0:
                t1_el = 1
        if len(allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team2)) 
                      & (allTeamResults["Season"] == date_season)
                      & (allTeamResults["Wettbewerb"] == "EL")  ] ) > 0:
                t2_el = 1
        
        # last resort to catch some small errors where team info from previous season is missing
        if (len(table_entry1) < 1) or (len(table_entry2) < 1):
            continue
        
        # append data to outDF
        outDF = outDF.append({'Retrieve_Date' : pendulum.now().to_date_string(),
                              'Game_Date' : pendulum_time.to_date_string(),
                
                             "Team1" : team1, 
                             "Team2" : team2, 
                             
                             "CurLeague" : cur_league, # current league
                             "Result" : result, # Game Endresult
                             
                             "Team1_Home" : t1_home, # 1 if Team 1 is Hometeam, else 0
                             "Team2_Home" : t2_home, # 1 if Team 2 is Hometeam, else 0
                             
                             "GameTimeOfDay" : gameTimeMinutes , 
                             "GameWeekday" : pendulum_time.day_of_week, # Weekday of Game
                             "GameDay" : gameDay, # GameDay in League
                             
                             "GamesSinceLastWin1" : last_game_won1, # No. of games since last won game Team1
                             "GamesSinceLastWin2" : last_game_won2, # No. of games since last won game Team2
                             
                             "TimeSinceLastGame1" : t_diff1, # Time in Hours since last game Team 1
                             "TimeSinceLastGame2" : t_diff2, # Time in Hours since last game Team 2
                             
                             "LastGameOverTime1" : t1_overtime, # Was last game of Team 1 with overtime or penalty shootout
                             "LastGameOverTime2" : t2_overtime, # Was last game of Team 2 with overtime or penalty shootout
                             
                             "TimeSinceLastCoach1" : t1_coach_diff, # Time since Team 1 has current coach (if any)
                             "TimeSinceLastCoach2" : t2_coach_diff, # Time since Team 2 has current coach (if any)    
                             
                             
                             "CurrentPoints1" : table_entry1["points"].values[0], # current position in league Team 1
                             "CurrentPoints2" : table_entry2["points"].values[0], # current position in league Team 2                          
                             
                             "CurrentPosition1" : table_entry1["rank"].values[0], # current position in league Team 1
                             "CurrentPosition2" : table_entry2["rank"].values[0], # current position in league Team 2
                             
                             "CurrentGoalDif1" : table_entry1["diff"].values[0], # current goal difference Team 1
                             "CurrentGoalDif2" : table_entry2["diff"].values[0], # current goal difference Team 2
                             
                             "CurrentWin1" : table_entry1["g"].values[0], # current wins in season of Team 1
                             "CurrentDraws1" : table_entry1["u"].values[0], # current draws in season of Team 1
                             "CurrentLoss1" : table_entry1["v"].values[0], # current losses in season of Team 1
                             "CurrentWin2" : table_entry2["g"].values[0], # current wins in season of Team 2
                             "CurrentDraws2" : table_entry2["u"].values[0], # current draws in season of Team 2
                             "CurrentLoss2": table_entry2["v"].values[0], # current losses in season of Team 2                             
                                                          
                             "LastSeasonPosition1" : lsp1 , # last season's final position in league Team 1
                             "LastSeasonPosition2" : lsp2 , # last season's final position in league Team 2
                             
                             "LastSeasonLeague1" : lsl1, # last season league of Team 1
                             "LastSeasonLeague2" : lsl2, # last season league of Team 2
                             
                             "Past5YearsInThisLeague1" : t1_last5, # 1 if Team1 played in same league for past 5 years, else 0
                             "Past5YearsInThisLeague2" : t2_last5 , # 1 if Team2 played in same league for past 5 years, else 0 
                             
                             "LastDirectGame1" : last_direct_1, # Last direct meeting of both teams results (0:0 if none)
                             "LastDirectGame2" : last_direct_2, # 2nd last direct meeting of both teams results (0:0 if none)
                             "LastDirectGame3" : last_direct_3, # 3rd last direct meeting of both teams results (0:0 if none)
                             
                             "LastDirectGame1_time" : last_direct_1_time, # Time in days since last direct meeting of both teams results (99999 if none)
                             "LastDirectGame2_time" : last_direct_2_time, # Time in days since 2nd last direct meeting of both teams results (99999 if none)
                             "LastDirectGame3_time" : last_direct_3_time, # Time in days since 3rd last direct meeting of both teams results (99999 if none)
                                 
                             "LastGameTeam1_1" : l5Games1[0], # Last 5 game results of Team 1                                 
                             "LastGameTeam1_2" : l5Games1[1],
                             "LastGameTeam1_3" : l5Games1[2],
                             "LastGameTeam1_4" : l5Games1[3],
                             "LastGameTeam1_5" : l5Games1[4],
                             "LastGameTeam2_1" : l5Games2[0], # Last 5 game results of Team 2
                             "LastGameTeam2_2" : l5Games2[1],
                             "LastGameTeam2_3" : l5Games2[2],
                             "LastGameTeam2_4" : l5Games2[3],
                             "LastGameTeam2_5" : l5Games2[4],
                             
                             "CL_candidate1" : t1_cl,  # Team 1 playing Champions League this season
                             "CL_candidate2" : t2_cl,  # Team 2 playing Champions League this season
                             
                             "EL_candidate1" : t1_el,  # Team 1 playing Europe League this season
                             "EL_candidate2" : t2_el   # Team 2 playing Europe League this season
                             },
                    ignore_index=True)
                                            
    outDF = switch_teams(outDF)
    outDF.to_csv(outFile, sep=";", encoding="utf8", index=False)
    
    


    
def build_ml_df(human_csv="human_table.csv", ml_csv="ml.csv", alias_json=alias_json):
    """
    Will convert all categorial variables into numeric values. Right now:
        - Team Names get number code used by kicker -> potential problem due to ordinal scale, 
                                                       may implement one-hot encoding in future
        - Results: will create 2 new variables: result_team1_win (1 if Team1 has won this game)
                                                result_diff (goal difference, negative if team 1 lost)
    
    Will return 1 Dataframe (and save to ml.csv) to be used in 2 different models 
    (exclude one variable in model, else it will be included for modelling):
        1 = predicted variable is goals of Team1
        2 = predicted variable is goal difference (negative if team 1 lost)
    """
    
    print("Creating ML Frame")
    
    # input can be a csv file or a dataframe
    if type(human_csv) == str:
        human_df = pd.read_csv(human_csv, sep=";", encoding="utf8")
    else:
        human_df = human_csv
    
    # switch team1 and team2, so team1 is always home team, delete IsHome columns after
    #human_df = switch_teams(human_df)
    
    
    # build a dictionary to convert team names to numerical kicker id using alias_json
    id_dict = {}
    for key in alias_json:
        kicker_id_name = [ x for x in alias_json[key] if "-" in x ][0]
        kicker_id = int(kicker_id_name[ kicker_id_name.rfind("-") + 1 : ])
        id_dict[key] = kicker_id
    
    # replace team names by ids
    human_df = human_df.replace(id_dict)
    
    # list of all columns containing results to be split
    split_list =  ["Result", 'LastDirectGame1', 'LastDirectGame2', 'LastDirectGame3',
                    'LastGameTeam1_1', 'LastGameTeam1_2',
                    'LastGameTeam1_3', 'LastGameTeam1_4', 'LastGameTeam1_5',
                    'LastGameTeam2_1', 'LastGameTeam2_2', 'LastGameTeam2_3',
                    'LastGameTeam2_4', 'LastGameTeam2_5']
    
    for col in split_list:
        if col not in human_df.columns:
            continue
        
        # splits result, puts into new cols named 0 and 1
        split_part = human_df[col].str.split(":", expand=True) 
        
        # overwrite column 1 (containing goals of team 2) with goal difference
        split_part[1] = split_part[0].astype(int) - split_part[1].astype(int) 
        
        # rename columns and append back on original dataframe        
        split_part.rename(columns={0:col+"_t1goals", 1:col+"_goaldiff"}, inplace=True)        
        human_df = human_df.join(split_part)
    
    # drop non numeric results columns
    split_list = [i for i in split_list if i in human_df.columns]
    human_df.drop(split_list + ['Retrieve_Date', 'Game_Date'], axis=1, inplace=True)
    
    if ml_csv == None:
        return human_df
    else:
        human_df.to_csv(ml_csv, sep=";", index=False)
    
    
    
    
    
    
def gameDayGames(gameday, league):
    """
    returns list of 9 Games of requested gameday using AllTeamResults.csv
    
    :gameday: :league: both (int) values for list to be gathered
    """
    
    
    cur_season = data_gathering.getCurrentSeason()
    
    # filter by league and season
    league_dict = {1: "BL", 2: "2.BL"}
    curSeason_TeamResults = allTeamResults[ (allTeamResults["Season"]==cur_season) &
                                            (allTeamResults["Wettbewerb"] ==  league_dict[league])]
    
    # get numerical column for gameday
    curSeason_TeamResults["gd"] =  curSeason_TeamResults.apply(lambda row: int(row["Spt./Runde"][ : row["Spt./Runde"].find(".")]), 
                                                               axis=1)
    
    # filter by passed gameday     
    relevant_TeamResults = curSeason_TeamResults[ (curSeason_TeamResults["gd"] == gameday)  ]
    
    # get kicker id for home team
    relevant_TeamResults["id1"] = relevant_TeamResults.apply(lambda row: int(row["Team"][ row["Team"].rfind("-") + 1 : ]), 
                                                               axis=1)
    # get kicker id for Gegner
    relevant_TeamResults["id2"] = relevant_TeamResults.apply(lambda row: int(getKickerTeamName(row["Gegner"])[ getKickerTeamName(row["Gegner"]).rfind("-") + 1 : ]), 
                                                               axis=1)
    
    # build a unique ID combo of both, smaller number first
    relevant_TeamResults["uid"] = relevant_TeamResults.apply(lambda row: str(min(row["id1"], row["id2"]))+
                                                                         str(max(row["id1"], row["id2"])  ), 
                                                                axis=1)
    
    # drop duplicates so each game is exactly once present
    relevant_TeamResults.drop_duplicates("uid", inplace=True)
    
    # get Date field in this step, to enable sorting
    relevant_TeamResults["Date"] = pd.to_datetime(relevant_TeamResults["Termin"].str.slice(4), errors='coerce', format='%d.%m.%y %H:%M')
    
    relevant_TeamResults.drop(["Unnamed: 0", "id1", "id2", "uid"], axis=1, inplace=True)
    
    return relevant_TeamResults
    
    

    
def buildPredictDF(inDF, allTeamResults=allTeamResults):
    """
    Builds an Array for each game in inDF that can be passed to model to make prediction.
    Does for each single game what createHumanFrame and build_ml_df do for entire training set
    
    :inDF: output of gameDayGames()
    """    
    
    print("Building Prediction DF")
    
    # create same human readable DF as with createHumanFrame
    outDF = pd.DataFrame(columns=['Retrieve_Date',
                                  'Game_Date',
                                  
                                 "Team1", 
                                 "Team2", 
                                 
                                 "CurLeague", # Current League
                                 
                                 "Team1_Home", # 1 if Team 1 is Hometeam, else 0
                                 "Team2_Home", # 1 if Team 2 is Hometeam, else 0
                                 
                                 "GameTimeOfDay", # Time of Game
                                 "GameWeekday", # Weekday of Game
                                 "GameDay", # GameDay in League
                                 
                                 "GamesSinceLastWin1", # No. of games since last won game Team1
                                 "GamesSinceLastWin2", # No. of games since last won game Team2
                                 
                                 "TimeSinceLastGame1", # Time in Hours since last game Team 1
                                 "TimeSinceLastGame2", # Time in Hours since last game Team 2
                                 
                                 "LastGameOverTime1", # Was last game of Team 1 with overtime or penalty shootout
                                 "LastGameOverTime2", # Was last game of Team 2 with overtime or penalty shootout
                                 
                                 "TimeSinceLastCoach1", # Time since Team 1 has current coach (if any)
                                 "TimeSinceLastCoach2", # Time since Team 2 has current coach (if any)
                                 
                                 "CurrentPoints1", # current position in league Team 1
                                 "CurrentPoints2", # current position in league Team 2
                                 
                                 "CurrentPosition1", # current position in league Team 1
                                 "CurrentPosition2", # current position in league Team 2
                                 
                                 "CurrentGoalDif1", # current goal difference Team 1
                                 "CurrentGoalDif2", # current goal difference Team 2
                                 
                                 "CurrentWin1", # current wins in season of Team 1
                                 "CurrentDraws1", # current draws in season of Team 1
                                 "CurrentLoss1", # current losses in season of Team 1
                                 "CurrentWin2", # current wins in season of Team 2
                                 "CurrentDraws2", # current draws in season of Team 2
                                 "CurrentLoss2", # current losses in season of Team 2
                                 
                                 "LastSeasonPosition1", # last season's final position in league Team 1
                                 "LastSeasonPosition2", # last season's final position in league Team 2
                                 
                                 "LastSeasonLeague1", # last season league of Team 1
                                 "LastSeasonLeague2", # last season league of Team 2
                                 
                                 "Past5YearsInThisLeague1", # 1 if Team1 played in same league for past 5 years, else 0
                                 "Past5YearsInThisLeague2", # 1 if Team2 played in same league for past 5 years, else 0
                                 
                                 "LastDirectGame1", # Last direct meeting of both teams results (0:0 if none)
                                 "LastDirectGame2", # 2nd last direct meeting of both teams results (0:0 if none)
                                 "LastDirectGame3", # 3rd last direct meeting of both teams results (0:0 if none)
                                 
                                 "LastDirectGame1_time", # Time in days since last direct meeting of both teams results (99999 if none)
                                 "LastDirectGame2_time", # Time in days since 2nd last direct meeting of both teams results (99999 if none)
                                 "LastDirectGame3_time", # Time in days since 3rd last direct meeting of both teams results (99999 if none)
                                 
                                 "LastGameTeam1_1", # Last 5 game results of Team 1                                 
                                 "LastGameTeam1_2",
                                 "LastGameTeam1_3",
                                 "LastGameTeam1_4",
                                 "LastGameTeam1_5",
                                 "LastGameTeam2_1", # Last 5 game results of Team 2
                                 "LastGameTeam2_2",
                                 "LastGameTeam2_3",
                                 "LastGameTeam2_4",
                                 "LastGameTeam2_5",
                                 
                                 "CL_candidate1", # Team 1 playing Champions League this season
                                 "CL_candidate2", # Team 2 playing Champions League this season
                                 
                                 "EL_candidate1", # Team 1 playing Europe League this season
                                 "EL_candidate2", # Team 2 playing Europe League this season
                                 ])
    
    
    # convert Termin column to new DateTime type column Date
    inDF["Date"] = pd.to_datetime(inDF["Termin"].str.slice(4), errors='coerce', format='%d.%m.%y %H:%M')
    allTeamResults["Date"] = pd.to_datetime(allTeamResults["Termin"].str.slice(4), errors='coerce', format='%d.%m.%y %H:%M')
    
    # split x:x into two columns T1Goals for Hometeam Goals and T2Goals for away team goals
    allTeamResults = allTeamResults.join(allTeamResults["Score"].str.split(":", expand=True)
                        ).rename(columns={0:"T1Goals", 1:"T2Goals"})    
    allTeamResults = allTeamResults.replace('-', np.nan)
    # calculate field with goal difference
    allTeamResults["Intmd"] = allTeamResults["T1Goals"].astype(float, errors='ignore').subtract(allTeamResults["T2Goals"].astype(float, errors='ignore'))
    # divide by its self absolute value ->  team1 win == 1, draw == 0, loss = -1
    allTeamResults["IsWin"] = allTeamResults["Intmd"].divide( allTeamResults["Intmd"].abs())
    # replace division by 0 values with 0, but only for existing results
    allTeamResults["IsWin"] = allTeamResults[allTeamResults["Score"] != "-:-"]["IsWin"].fillna(0)
    
    
    # iterate over allTeamResults and extract infos for each game
    for row_tup in inDF.iterrows():
        row = row_tup[1]
        
        pendulum_time = pendulum.from_format(row["Termin"][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin')  

        date_season = seasonFromDate(pendulum_time)
        
        team1 = translateTeam( row["Team"] )
        team2 = translateTeam( row["Gegner"] )
        
        print('{} vs. {} built'.format(team1, team2))
        
        # determine numeric league
        if row["Wettbewerb"] == 'BL':
            cur_league = 1
        else:
            cur_league = 2
        #result = row["Score"] 
        
        # get home team 
        if row["Wo"] == "H":
            t1_home = 1
            t2_home = 0
        elif row["Wo"] == "A":
            t1_home = 0
            t2_home = 1
            
        # game time in minutes since midnight, e.g. 13:00h == 780
        gameTimeMinutes = pendulum_time.hour * 60 + pendulum_time.minute 
        
        # get gameDay, only works for BL gamedays
        gameDay = int(row["Spt./Runde"][ : row["Spt./Runde"].find(".")]) 
        
        
        # get df with only current team and only games WITH result 
        lf_df1 = allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team1)) ]#& (allTeamResults["Score"] != "-:-") ]
        lf_df2 = allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team2)) ]#& (allTeamResults["Score"] != "-:-") ]
        # sort lf_df by date, so last games are on bottom
        lf_df1 = lf_df1.sort_values("Date")        
        lf_df2 = lf_df2.sort_values("Date") 
        # cut_off all games after current
        lf_df1_cur = lf_df1[lf_df1["Date"] < pendulum_time.to_datetime_string()]  
        lf_df2_cur = lf_df2[lf_df2["Date"] < pendulum_time.to_datetime_string()] 
        # get last game with win (0 if last game was win) - reverse IsWin column as list and return index of first element that is 1
        last_game_won1 = lf_df1_cur["IsWin"].tolist()[::-1].index(1)
        last_game_won2 = lf_df2_cur["IsWin"].tolist()[::-1].index(1)
        
        # get time since last game in hours       
        lf_df1_reidx = lf_df1.reset_index()
        lf_df2_reidx = lf_df2.reset_index()
         # get index of row above current row
        t1_idx = lf_df1_reidx[lf_df1_reidx["Termin"] == row["Termin"]].index - 1
        t2_idx = lf_df2_reidx[lf_df2_reidx["Termin"] == row["Termin"]].index - 1 
        # get game time with index above
        last_game_time1 = pendulum.from_format(lf_df1_reidx.iloc[t1_idx]["Termin"].values[0][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin') 
        last_game_time2 = pendulum.from_format(lf_df2_reidx.iloc[t2_idx]["Termin"].values[0][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin')      
        # get difference to current game in hours
        t_diff1 = (pendulum_time - last_game_time1).in_hours()
        t_diff2 = (pendulum_time - last_game_time2).in_hours()
        # was last game overtime 
        t1_overtime = lf_df1_reidx.iloc[t1_idx]["Overtime"].values[0]
        t2_overtime = lf_df2_reidx.iloc[t2_idx]["Overtime"].values[0]
        
        # time since last coach
        t1_coaches = allCoaches[(allCoaches["Team"] == getKickerTeamName(team1)) & (allCoaches["von"] < pendulum_time.to_date_string())
                               ].sort_values("von")
        t2_coaches = allCoaches[(allCoaches["Team"] == getKickerTeamName(team2)) & (allCoaches["von"] < pendulum_time.to_date_string())
                               ].sort_values("von")
        # time difference in days between game and last coach recruiting
        try:
            t1_coach_diff = (pendulum_time - pendulum.instance(t1_coaches.iloc[-1]["von"], tz='Europe/Berlin')).in_days()
        except:
            t1_coach_diff = 99999
        try:
            t2_coach_diff = (pendulum_time - pendulum.instance(t2_coaches.iloc[-1]["von"], tz='Europe/Berlin')).in_days()
        except:
            t2_coach_diff = 99999
        
        # Get last 5 games as list
        l5Games1 = []
        l5Games2 = []
        lf_df1_cur = lf_df1_cur[lf_df1_cur["Score"] != "-:-"].reset_index()
        lf_df2_cur = lf_df2_cur[lf_df2_cur["Score"] != "-:-"].reset_index()
        for g in range(0,5):
            #t1_idx = lf_df1_reidx[lf_df1_reidx["Termin"] == row["Termin"]].index - g
            t1_idx = lf_df1_cur.index[-1] - g
            l5Games1.append(lf_df1_cur.iloc[t1_idx]["Score"])
            #t2_idx = lf_df2_reidx[lf_df2_reidx["Termin"] == row["Termin"]].index - g 
            t2_idx = lf_df2_cur.index[-1] - g
            l5Games2.append(lf_df2_cur.iloc[t2_idx]["Score"])     
        
        # Get last 3 direct games between both teams (manually account for teams that havent met 3 times, set 0:0 default)
        last_direct_df = lf_df1_reidx[ (lf_df1_reidx["Gegner"] == team2) & (lf_df1_reidx["Date"] < pendulum_time.to_datetime_string())].sort_values("Date")
        
        if len(last_direct_df) == 0:
            last_direct_3 = "0:0"
            last_direct_3_time = 99999
            last_direct_2 = "0:0"
            last_direct_2_time = 99999
            last_direct_1 = "0:0"
            last_direct_1_time = 99999
            
        
        elif len(last_direct_df) == 1:
            last_direct_3 = "0:0"
            last_direct_3_time = 99999
            last_direct_2 = "0:0"
            last_direct_2_time = 99999
            
            last_direct_1 = last_direct_df.iloc[-1]["Score"]
            last_direct_1_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-1]["Date"])).in_days()
        
        elif len(last_direct_df) == 2:
            last_direct_3 = "0:0"
            last_direct_3_time = 99999
            
            last_direct_2 = last_direct_df.iloc[-2]["Score"]
            last_direct_2_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-2]["Date"])).in_days()
            
            last_direct_1 = last_direct_df.iloc[-1]["Score"]
            last_direct_1_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-1]["Date"])).in_days()
        
        else:
            last_direct_3 = last_direct_df.iloc[-3]["Score"]
            last_direct_3_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-3]["Date"])).in_days()
            
            last_direct_2 = last_direct_df.iloc[-2]["Score"]
            last_direct_2_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-2]["Date"])).in_days()
            
            last_direct_1 = last_direct_df.iloc[-1]["Score"]
            last_direct_1_time = (pendulum_time - pendulum.instance(last_direct_df.iloc[-1]["Date"])).in_days()
        
        # table entry for date
        if gameDay > 1:
            table_entry1 = allTables[ (allTables["Team"] == team1) & (allTables["Season"] == date_season) & (allTables["GameDay"] == gameDay-1) ]
            table_entry2 = allTables[ (allTables["Team"] == team2) & (allTables["Season"] == date_season) & (allTables["GameDay"] == gameDay-1) ]
            
        # create dummy df with all 0 data for first gameday
        else:
            table_entry1 = pd.DataFrame(data={'Season':date_season, 'League': cur_league, 'GameDay':gameDay, 'rank':0,
           'Team':team1, 'sp':0, 'g':0, 'u':0, 'v':0, 'tore':0, 'diff':0, 'points':0}, index=[0])
            table_entry2 = pd.DataFrame(data={'Season':date_season, 'League': cur_league, 'GameDay':gameDay, 'rank':0,
           'Team':team2, 'sp':0, 'g':0, 'u':0, 'v':0, 'tore':0, 'diff':0, 'points':0}, index=[0])
        
        # last seasons last gameday entry
        ls_table_entry1 = allTables[ (allTables["Team"] == team1) & (allTables["Season"] == date_season-1) & (allTables["GameDay"] == 34) ]
        ls_table_entry2 = allTables[ (allTables["Team"] == team2) & (allTables["Season"] == date_season-1) & (allTables["GameDay"] == 34) ]
        
        # last 5 seasons league, 1 if all same as current league, 0 if at least one season was different
        t1_last5, t2_last5 = 0,0
        if (len(set(getPastLeagues(team1, date_season))) == 1) & (getPastLeagues(team1, date_season)[0]==table_entry1["League"].values[0]):
            t1_last5 = 1
        if (len(set(getPastLeagues(team2, date_season))) == 1) & (getPastLeagues(team2, date_season)[0]==table_entry2["League"].values[0]):
            t2_last5 = 1
        
        # last season positions, if no table exists, default to 3   
        try:
            lsp1 = ls_table_entry1["rank"].values[0]
        except:
            lsp2 = 3
        try:
            lsp2 = ls_table_entry2["rank"].values[0]
        except:
            lsp2 = 3
        
        # last season league, if no table exists, default to 3   
        try:
            lsl1 = ls_table_entry1["League"].values[0]
        except:
            lsl2 = 3
        try:
            lsl2 = ls_table_entry2["League"].values[0]
        except:
            lsl2 = 3
        
        # get if playing in CL or EL in current season
        t1_cl, t2_cl, t1_el, t2_el = 0,0,0,0
        if len(allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team1)) 
                      & (allTeamResults["Season"] == date_season)
                      & (allTeamResults["Wettbewerb"] == "CL")  ] ) > 0:
                t1_cl = 1
        if len(allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team2)) 
                      & (allTeamResults["Season"] == date_season)
                      & (allTeamResults["Wettbewerb"] == "CL")  ] ) > 0:
                t2_cl = 1
        if len(allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team1)) 
                      & (allTeamResults["Season"] == date_season)
                      & (allTeamResults["Wettbewerb"] == "EL")  ] ) > 0:
                t1_el = 1
        if len(allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team2)) 
                      & (allTeamResults["Season"] == date_season)
                      & (allTeamResults["Wettbewerb"] == "EL")  ] ) > 0:
                t2_el = 1
        
        # append data to outDF
        outDF = outDF.append({'Retrieve_Date' : pendulum.now().to_date_string(),
                              'Game_Date' : pendulum_time.to_date_string(),
                
                             "Team1" : team1, 
                             "Team2" : team2, 
                             
                             "CurLeague" : cur_league, # current league
                             
                             "Team1_Home" : t1_home, # 1 if Team 1 is Hometeam, else 0
                             "Team2_Home" : t2_home, # 1 if Team 2 is Hometeam, else 0
                             
                             "GameTimeOfDay" : gameTimeMinutes , 
                             "GameWeekday" : pendulum_time.day_of_week, # Weekday of Game
                             "GameDay" : gameDay, # GameDay in League
                             
                             "GamesSinceLastWin1" : last_game_won1, # No. of games since last won game Team1
                             "GamesSinceLastWin2" : last_game_won2, # No. of games since last won game Team2
                             
                             "TimeSinceLastGame1" : t_diff1, # Time in Hours since last game Team 1
                             "TimeSinceLastGame2" : t_diff2, # Time in Hours since last game Team 2
                             
                             "LastGameOverTime1" : t1_overtime, # Was last game of Team 1 with overtime or penalty shootout
                             "LastGameOverTime2" : t2_overtime, # Was last game of Team 2 with overtime or penalty shootout
                             
                             "TimeSinceLastCoach1" : t1_coach_diff, # Time since Team 1 has current coach (if any)
                             "TimeSinceLastCoach2" : t2_coach_diff, # Time since Team 2 has current coach (if any)    
                             
                             
                             "CurrentPoints1" : table_entry1["points"].values[0], # current position in league Team 1
                             "CurrentPoints2" : table_entry2["points"].values[0], # current position in league Team 2                          
                             
                             "CurrentPosition1" : table_entry1["rank"].values[0], # current position in league Team 1
                             "CurrentPosition2" : table_entry2["rank"].values[0], # current position in league Team 2
                             
                             "CurrentGoalDif1" : table_entry1["diff"].values[0], # current goal difference Team 1
                             "CurrentGoalDif2" : table_entry2["diff"].values[0], # current goal difference Team 2
                             
                             "CurrentWin1" : table_entry1["g"].values[0], # current wins in season of Team 1
                             "CurrentDraws1" : table_entry1["u"].values[0], # current draws in season of Team 1
                             "CurrentLoss1" : table_entry1["v"].values[0], # current losses in season of Team 1
                             "CurrentWin2" : table_entry2["g"].values[0], # current wins in season of Team 2
                             "CurrentDraws2" : table_entry2["u"].values[0], # current draws in season of Team 2
                             "CurrentLoss2": table_entry2["v"].values[0], # current losses in season of Team 2                             
                                                          
                             "LastSeasonPosition1" : lsp1 , # last season's final position in league Team 1
                             "LastSeasonPosition2" : lsp2 , # last season's final position in league Team 2
                             
                             "LastSeasonLeague1" : lsl1, # last season league of Team 1
                             "LastSeasonLeague2" : lsl2, # last season league of Team 2
                             
                             "Past5YearsInThisLeague1" : t1_last5, # 1 if Team1 played in same league for past 5 years, else 0
                             "Past5YearsInThisLeague2" : t2_last5 , # 1 if Team2 played in same league for past 5 years, else 0 
                             
                             "LastDirectGame1" : last_direct_1, # Last direct meeting of both teams results (0:0 if none)
                             "LastDirectGame2" : last_direct_2, # 2nd last direct meeting of both teams results (0:0 if none)
                             "LastDirectGame3" : last_direct_3, # 3rd last direct meeting of both teams results (0:0 if none)
                             
                             "LastDirectGame1_time" : last_direct_1_time, # Time in days since last direct meeting of both teams results (99999 if none)
                             "LastDirectGame2_time" : last_direct_2_time, # Time in days since 2nd last direct meeting of both teams results (99999 if none)
                             "LastDirectGame3_time" : last_direct_3_time, # Time in days since 3rd last direct meeting of both teams results (99999 if none)
                                 
                             "LastGameTeam1_1" : l5Games1[0], # Last 5 game results of Team 1                                 
                             "LastGameTeam1_2" : l5Games1[1],
                             "LastGameTeam1_3" : l5Games1[2],
                             "LastGameTeam1_4" : l5Games1[3],
                             "LastGameTeam1_5" : l5Games1[4],
                             "LastGameTeam2_1" : l5Games2[0], # Last 5 game results of Team 2
                             "LastGameTeam2_2" : l5Games2[1],
                             "LastGameTeam2_3" : l5Games2[2],
                             "LastGameTeam2_4" : l5Games2[3],
                             "LastGameTeam2_5" : l5Games2[4],
                             
                             "CL_candidate1" : t1_cl,  # Team 1 playing Champions League this season
                             "CL_candidate2" : t2_cl,  # Team 2 playing Champions League this season
                             
                             "EL_candidate1" : t1_el,  # Team 1 playing Europe League this season
                             "EL_candidate2" : t2_el   # Team 2 playing Europe League this season
                             },
                    ignore_index=True)
    
    return outDF
    