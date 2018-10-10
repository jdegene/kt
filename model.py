# -*- coding: utf-8 -*-

# handles data transformation, model building and model application

import numpy as np
import pandas as pd
import json

import pendulum



# # # # # # # # # LOAD RAW DATA # # # # # # # # #

allTeamPages = pd.read_csv("E:/Test/AllTeamPages.csv", sep=";")
allTeamResults = pd.read_csv("E:/Test/AllTeamResults.csv", sep=";")
allTables = pd.read_csv("E:/Test/AllTables.csv", sep=";")
allCoaches = pd.read_csv("E:/Test/AllTeamCoaches.csv", sep=";")

with open("C:/WorkExchange/Python/Git/kt/alias.json", "r", encoding="utf8") as j:
    alias_json = json.load( j )


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
    if penudulum_time.month < 8:
        cur_season = penudulum_time.year - 1999
    else:
        cur_season = penudulum_time.year - 2000
    return cur_season



# # # # # # # # # BUILD INPUT DF # # # # # # # # #

def createMainFrame():
    """
    Use basic data (data_gathering.py output) to create comprehensive DataFrame 
    for actual modelling
    """
    
    # create final output DataFrame
    outDF = pd.DataFrame(colums=["Team1", 
                                 "Team2", 
                                 
                                 "GameTimeOfDay", # Time of Game
                                 "GameWeekday", # Weekday of Game
                                 "GameDay", # GameDay in League
                                 
                                 "GamesSinceLastWin1", # No. of games since last won game Team1
                                 "GamesSinceLastWin2", # No. of games since last won game Team2

                                 "TimeSinceLastGame1", # Time in Hours since last game Team 1
                                 "TimeSinceLastGame2", # Time in Hours since last game Team 2
                                 
                                 "TimeSinceLastCoach1", # Time since Team 1 has current coach (if any)
                                 "TimeSinceLastCoach2", # Time since Team 2 has current coach (if any)
                                 
                                 "CurrentPoints1", # current position in league Team 1
                                 "CurrentPoints2", # current position in league Team 2
                                 
                                 "CurrentPosition1", # current position in league Team 1
                                 "CurrentPosition2", # current position in league Team 2
                                 
                                 "CurrentGoalDif1", # current goal difference Team 1
                                 "CurrentGoalDif2", # current goal difference Team 2
                                 
                                 "CurrentGoalDif1", # current goal difference Team 1
                                 "CurrentGoalDif2", # current goal difference Team 2
                                 
                                 "LastSeasonPosition1", # last season's final position in league Team 1
                                 "LastSeasonPosition2", # last season's final position in league Team 2
                                 
                                 "LastSeasonLeague1", # last season league of Team 1
                                 "LastSeasonLeague2", # last season league of Team 2
                                 
                                 "Past5YearsInThisLeague1", # No of season played in this league last 5 years by Team 1
                                 "Past5YearsInThisLeague2", # No of season played in this league last 5 years by Team 2
                                 
                                 "LastDirectGame1", # Last direct meeting of both teams results (0:0 if none)
                                 "LastDirectGame2", # 2nd last direct meeting of both teams results (0:0 if none)
                                 "LastDirectGame3", # 3rd last direct meeting of both teams results (0:0 if none)
                                 
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
                                 
                                 "LastGameOverTime1", # Was last game of Team 1 with overtime or penalty shootout
                                 "LastGameOverTime2", # Was last game of Team 2 with overtime or penalty shootout
                                 
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
    # divide by its self absolute value -> Hometeam win == 1, draw == 0, loss = -1
    allTeamResults["IsWin"] = allTeamResults["Intmd"].divide( allTeamResults["Intmd"].abs())
    # replace division by 0 values with 0, but only for existing results
    allTeamResults["IsWin"] = allTeamResults[allTeamResults["Score"] != "-:-"]["IsWin"].fillna(0)
    
    
    # convert Termin column to new DateTime type column Date
    allTeamResults["Date"] = pd.to_datetime(allTeamResults["Termin"].str.slice(4), errors='coerce', format='%d.%m.%y %H:%M')
    
    
    # build human readale dataframe (= don't transform categorical data yet)
    
    # list will hold tuples of games, if same game is found in another teams list, its not put into df again
    skip_list = [] 
    
    # iterate over allTeamResults and extract infos for each game
    for row_tup in allTeamResults.iterrows():
        row = row_tup[1] # original row returns a tuple with first elem as index, second elem as data
        
        # skip adding if game was not in 1st or 2nd BL
        if row["Wettbewerb"] not in ['BL', '2.BL']:
            continue
        
        team1 = translateTeam( row["Team"] )
        team2 = translateTeam( row["Gegner"] )
        
        # skip game if already in list, if not in list, append to list then continue
        if (team2, team1, row["Termin"]) in skip_list:
            continue        
        skip_list.append( (team1, team2, row["Termin"]) )
        
        

    
       
        # # # time-related caluclations # # # 
        penudulum_time = pendulum.from_format(row["Termin"][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin')     
        
        # game time in minutes since midnight, e.g. 13:00h == 780
        gameTimeMinutes = penudulum_time.hour * 60 + penudulum_time.minute
        
        
        gameDay = int(row["Spt./Runde"][ : row["Spt./Runde"].find(".")]) # Only works with BL gamedays
        
        
        # get no of games since last win
        
        # get df with only current team and only games WITH result 
        lf_df1 = allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team1)) & (allTeamResults["Score"] != "-:-") ]
        lf_df2 = allTeamResults[ (allTeamResults["Team"] == getKickerTeamName(team2)) & (allTeamResults["Score"] != "-:-") ]
        # sort lf_df by date, so last games are on bottom
        lf_df1 = lf_df1.sort_values("Date")        
        lf_df2 = lf_df2.sort_values("Date") 
        # get last game with win (0 if last game was win) - reverse IsWin column as list and return index of first element that is 1
        last_game_won1 = lf_df["IsWin"].tolist()[::-1].index(1)
        last_game_won2 = lf_df2["IsWin"].tolist()[::-1].index(1)
        
        

        
    
    
        # append data to outDF
        outDF = outDF.append({"Team1" : team1, 
                             "Team2" : team2, 
                             
                             "GameTimeOfDay" : gameTimeMinutes , 
                             "GameWeekday" : penudulum_time.day_of_week, # Weekday of Game
                             "GameDay" : gameDay, # GameDay in League
                             
                             "GamesSinceLastWin1" : last_game_won1, # No. of games since last won game Team1
                             "GamesSinceLastWin2" : last_game_won2, # No. of games since last won game Team2
                             
                             "CurrentPoints1", # current position in league Team 1
                             "CurrentPoints2", # current position in league Team 2
                             
                             "TimeSinceLastGame1", # Time in Hours since last game Team 1
                             "TimeSinceLastGame2", # Time in Hours since last game Team 2
                             
                             "TimeSinceLastCoach1", # Time since Team 1 has current coach (if any)
                             "TimeSinceLastCoach2"
                             
                             "CurrentPosition1", # current position in league Team 1
                             "CurrentPosition2", # current position in league Team 2
                             
                             "CurrentGoalDif1", # current goal difference Team 1
                             "CurrentGoalDif2", # current goal difference Team 2
                             
                             "CurrentGoalDif1", # current goal difference Team 1
                             "CurrentGoalDif2", # current goal difference Team 2
                             
                             "LastSeasonPosition1", # last season's final position in league Team 1
                             "LastSeasonPosition2", # last season's final position in league Team 2
                             
                             "LastSeasonLeague1", # last season league of Team 1
                             "LastSeasonLeague2", # last season league of Team 2
                             
                             "Past5YearsInThisLeague1", # No of season played in this league last 5 years by Team 1
                             "Past5YearsInThisLeague2", # No of season played in this league last 5 years by Team 2
                             
                             "LastDirectGame1", # Last direct meeting of both teams results (0:0 if none)
                             "LastDirectGame2", # 2nd last direct meeting of both teams results (0:0 if none)
                             "LastDirectGame3", # 3rd last direct meeting of both teams results (0:0 if none)
                             
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
                             
                             "LastGameOverTime1", # Was last game of Team 1 with overtime or penalty shootout
                             "LastGameOverTime2", # Was last game of Team 2 with overtime or penalty shootout
                             
                             "CL_candidate1", # Team 1 playing Champions League this season
                             "CL_candidate2", # Team 2 playing Champions League this season
                             
                             "EL_candidate1", # Team 1 playing Europe League this season
                             "EL_candidate2", # Team 2 playing Europe League this season
                             },
                    ignore_index=True)
    
    
    
    
    
    