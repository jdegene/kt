# -*- coding: utf-8 -*-

# Handles function calling for data update, data crunching, modeling and input to kt

import pandas as pd

import data_gathering
import build_dfs
import model




if __name__ == "__main__":
    
    data_folder = "C:/Stuff/Projects/kicktipp/"
    
    league_1_gameday = 8  
    league_2_gameday = 10
    
    
    # First update all data
    data_gathering.updateAll(allTeamPages =  data_folder + "AllTeamPages.csv", 
                             allTeamResults = data_folder + "AllTeamResults.csv", 
                             allTables = data_folder + "AllTables.csv", 
                             allCoaches = data_folder + "AllTeamCoaches.csv")
    
    
    
    # Create human df for upcoming games for both leagues
    league_1_games = build_dfs.gameDayGames(league_1_gameday, 1)
    human_df_1 = build_dfs.buildPredictDF(league_1_games)
    
    league_2_games = build_dfs.gameDayGames(league_2_gameday, 2)
    human_df_2 = build_dfs.buildPredictDF(league_2_games)
    
    # build machine df from both datasets
    ml_df1 = build_dfs.build_ml_df(human_csv=human_df_1, ml_csv=None)
    ml_df2 = build_dfs.build_ml_df(human_csv=human_df_2, ml_csv=None)
    
    
    # build models (for now, build new model for every run)    
    ml_df = pd.read_csv(data_folder + "ml.csv", sep=";")
    t1goals_model = model.create_t1goals_model(ml_df)
    goaldiff_model = model.create_goaldiff_model(ml_df)
    
    
    # feed predict arrays into models and return output
    print("League 1 results for GameDay", league_1_gameday, "\n")
    for row in ml_df1.iterrows():
        t1goals = t1goals_model.predict(row[1].values.reshape(1, -1))
        goaldiff = goaldiff_model.predict(row[1].values.reshape(1, -1))
        
        t1goals = max(t1goals, goaldiff) # account for cases, goaldiff is larger than shot goals
        
        team_str = human_df_1.iloc[row[0]]["Team1"] + " : " +  human_df_1.iloc[row[0]]["Team2"]
        team_str = team_str + ''.join([" " for x in range(40 - len(team_str))] ) 
        print(team_str + "\t--->\t", t1goals[0], ":", t1goals[0]-goaldiff[0])
    
    print("\nLeague 2 results for GameDay", league_2_gameday, "\n")
    for row in ml_df2.iterrows():
        t1goals = t1goals_model.predict(row[1].values.reshape(1, -1))
        goaldiff = goaldiff_model.predict(row[1].values.reshape(1, -1))
        
        t1goals = max(t1goals, goaldiff) # account for cases, goaldiff is larger than shot goals
        
        team_str = human_df_2.iloc[row[0]]["Team1"] + " : " +  human_df_2.iloc[row[0]]["Team2"]
        team_str = team_str + ''.join([" " for x in range(40 - len(team_str))] ) 
        print(team_str + "\t--->\t", t1goals[0], ":", t1goals[0]-goaldiff[0])
        
