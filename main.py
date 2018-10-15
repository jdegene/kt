# -*- coding: utf-8 -*-

# Handles function calling for data update, data crunching, modeling and input to kt
# Change league_1_gameday and league_2_gameday, then simply run entire file

import pandas as pd

import data_gathering
import build_dfs
import model


def getVotes(i):
    """
    Run model i times, use most prevalent outputs for goals and goaldiff
    """
    goalList = []
    diffList = []
    for i in range(i):

        t1goals_model = model.create_t1goals_model(ml_df, silent=1)
        goaldiff_model = model.create_goaldiff_model(ml_df,silent=1)
        
        t1goals = t1goals_model.predict(row[1].values.reshape(1, -1))
        goaldiff = goaldiff_model.predict(row[1].values.reshape(1, -1))
    
        t1goals = max(t1goals, goaldiff) 
        
        goalList.append(t1goals[0])
        diffList.append(goaldiff[0])
    
    most_common_goal = max(set(goalList), key=goalList.count)    
    most_common_diff = max(set(diffList), key=diffList.count)
    
    return [most_common_goal], [most_common_diff]



def makeTeamsOneHot(df, colList=None):
    """
    Transforms columns Team1 and Team2 of passed DF into one-hot encoded vectors
    For trainig data straightforward. For predict data some columns are missing 
        (only half the teams present in predict data-set -> 
             manually create dummy columns from colList, fill with 0)
        
    :colList: use column list from training ml dataset
    """    
    
    one_hot_df = pd.get_dummies(df, columns=["Team1","Team2"])
    
    if colList is not None:
        # remove y-variables from predicitive column list (they dont exist for future games)
        colList = [x for x in colList if x not in ["Result_goaldiff", "Result_t1goals"] ]
        for col in colList:
            if col not in one_hot_df:
                one_hot_df[col] = 0
        
        one_hot_df = one_hot_df[colList]
    
    return one_hot_df



if __name__ == "__main__":
    
    data_folder = "C:/Stuff/Projects/kicktipp/"
    
    league_1_gameday = 8  
    league_2_gameday = 10
    
    
    # First update all data
    data_gathering.updateAll(allTeamPages =  data_folder + "AllTeamPages.csv", 
                             allTeamResults = data_folder + "AllTeamResults.csv", 
                             allTables = data_folder + "AllTables.csv", 
                             allCoaches = data_folder + "AllTeamCoaches.csv")
          
    # get training dataset, make teams one-hot
    ml_df = pd.read_csv(data_folder + "ml.csv", sep=";")
    ml_df_oh = makeTeamsOneHot(ml_df)
    
    # build models (for now, build new model for every run)    
    t1goals_model = model.create_t1goals_model(ml_df_oh)
    goaldiff_model = model.create_goaldiff_model(ml_df_oh)
    
    
    # Create human df for upcoming games for both leagues
    league_1_games = build_dfs.gameDayGames(league_1_gameday, 1).sort_values("Date")
    human_df_1 = build_dfs.buildPredictDF(league_1_games)
    human_df_1 = build_dfs.switch_teams(human_df_1)
    
    league_2_games = build_dfs.gameDayGames(league_2_gameday, 2).sort_values("Date")
    human_df_2 = build_dfs.buildPredictDF(league_2_games)
    human_df_2 = build_dfs.switch_teams(human_df_2)
    
    # build machine df from both datasets
    ml_df1 = build_dfs.build_ml_df(human_csv=human_df_1, ml_csv=None)
    ml_df2 = build_dfs.build_ml_df(human_csv=human_df_2, ml_csv=None)
    
    # convert predict mls to one-hot
    ml_df1_oh = makeTeamsOneHot(ml_df1, colList = ml_df_oh.columns)
    ml_df2_oh = makeTeamsOneHot(ml_df2, colList = ml_df_oh.columns)

    
    
    # feed predict arrays into models and return output
    print("League 1 results for GameDay", league_1_gameday, "\n")
    for row in ml_df1_oh.iterrows():
        t1goals = t1goals_model.predict(row[1].values.reshape(1, -1))
        goaldiff = goaldiff_model.predict(row[1].values.reshape(1, -1))
        
        t1goals = max(t1goals, goaldiff) # account for cases, goaldiff is larger than shot goals
        
        # will produce at lot of draws
        #t1goals,goaldiff = getVotes(10)            
        
        team_str = human_df_1.iloc[row[0]]["Team1"] + " : " +  human_df_1.iloc[row[0]]["Team2"]
        team_str = team_str + ''.join([" " for x in range(40 - len(team_str))] ) 
        print(team_str + "\t--->\t", t1goals[0], ":", t1goals[0]-goaldiff[0])
    
    print("\nLeague 2 results for GameDay", league_2_gameday, "\n")
    for row in ml_df2_oh.iterrows():
        t1goals = t1goals_model.predict(row[1].values.reshape(1, -1))
        goaldiff = goaldiff_model.predict(row[1].values.reshape(1, -1))
        
        t1goals = max(t1goals, goaldiff) # account for cases, goaldiff is larger than shot goals
        
        team_str = human_df_2.iloc[row[0]]["Team1"] + " : " +  human_df_2.iloc[row[0]]["Team2"]
        team_str = team_str + ''.join([" " for x in range(40 - len(team_str))] ) 
        print(team_str + "\t--->\t", t1goals[0], ":", t1goals[0]-goaldiff[0])
        
