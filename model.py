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
                                 "LastDirectGame1", # Last direct meeting of both teams results
                                 "LastDirectGame2", # 2nd last direct meeting of both teams results
                                 "LastDirectGame3", # 3rd last direct meeting of both teams results
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
                                 
                                 "TimeSinceLastGame1", # Time in Hours since last game Team 1
                                 "TimeSinceLastGame2", # Time in Hours since last game Team 2
                                 
                                 "LastGameOverTime1", # Was last game of Team 1 with overtime or penalty shootout
                                 "LastGameOverTime2", # Was last game of Team 2 with overtime or penalty shootout
                                 ])