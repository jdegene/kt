# -*- coding: utf-8 -*-

# handles data transformation, model building and model application

import pandas as pd

import pendulum


def createMainFrame():
    """
    Use basic data (data_gathering.py output) to create comprehensive DataFrame 
    for actual modelling
    """
    
    # create final output DataFrame
    outDF = pd.DataFrame(colums=["Team1", 
                                 "Team2", 
                                 
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
                                 "Past5YearsInThisLeague1", # No of season played in this league last 5 years by Team 2
                                 
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
                                 
                                 "TimeSinceLastGame1", # Time in Hours since last game Team 1
                                 "TimeSinceLastGame2", # Time in Hours since last game Team 2
                                 
                                 "TimeSinceLastCoach1", # Time since Team 1 has current coach (if any)
                                 "TimeSinceLastCoach2", # Time since Team 2 has current coach (if any)
                                 
                                 "GameTimeOfDay", # Time of Game
                                 "GameWeekday", # Weekday of Game
                                 "GameDay", # GameDay in League
                                 ])