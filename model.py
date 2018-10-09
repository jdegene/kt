# -*- coding: utf-8 -*-

# handles data transformation, model building and model application

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

# # # # # # # # # BUILD INPUT DF # # # # # # # # #

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
        
        # time-wise caluclations
        penudulum_time = pendulum.from_format(roww["Termin"][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin')        
        # game time in minutes since midnight, e.g. 13:00h == 780
        gameTimeMinutes = penudulum_time.hour * 60 + penudulum_time.minute
        
        gameDay = int(row["Spt./Runde"][ : row["Spt./Runde"].find(".")])
        
    
    
        # append data to outDF
        outDF = outDF.append({"Team1" : team1, 
                             "Team2" : team2, 
                             
                             "GameTimeOfDay" : gameTimeMinutes , 
                             "GameWeekday" : penudulum_time.day_of_week, # Weekday of Game
                             "GameDay" : gameDay, # GameDay in League
                             
                             "GamesSinceLastWin1", # No. of games since last won game Team1
                             "GamesSinceLastWin2", # No. of games since last won game Team2
                             
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
    
    
    
    
    
    