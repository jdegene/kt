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

for col in ["von", "bis"]:
    allCoaches[col] = pd.to_datetime(allCoaches[col], errors="coerce", format="%d.%m.%Y")

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
    if pendulum_time.month < 8:
        cur_season = pendulum_time.year - 1999
    else:
        cur_season = pendulum_time.year - 2000
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




# # # # # # # # # BUILD INPUT DF # # # # # # # # #

#def createMainFrame():
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
    row_index = row_tup[0]
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
    pendulum_time = pendulum.from_format(row["Termin"][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin')   
    date_season = seasonFromDate(pendulum_time)
    
    # game time in minutes since midnight, e.g. 13:00h == 780
    gameTimeMinutes = pendulum_time.hour * 60 + pendulum_time.minute        
    
    # get gameDay, only works for BL gamedays
    gameDay = int(row["Spt./Runde"][ : row["Spt./Runde"].find(".")]) 
    
    
    
    
    
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
    
    
    # get time since last game in hours       
    lf_df1_reidx = lf_df1.reset_index()
    lf_df2_reidx = lf_df2.reset_index()
     # get index of row above current row
    t1_idx = lf_df1_reidx[lf_df1_reidx["Termin"] == row["Termin"]] - 1
    t2_idx = lf_df1_reidx[lf_df1_reidx["Termin"] == row["Termin"]] - 1 
    # get game time with index above
    last_game_time1 = pendulum.from_format(lf_df1_reidx[t1_idx]["Termin"].values[0][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin')  
    last_game_time2 = pendulum.from_format(lf_df2_reidx[t2idx]["Termin"].values[0][4:], 'DD.MM.YY HH:mm', tz='Europe/Berlin')  
    # get difference to current game in hours
    t_diff1 = (pendulum_time - last_game_time1).in_hours()
    t_diff2 = (pendulum_time - last_game_time2).in_hours()
    
    # was last game overtime 
    t1_overtime = t1_idx["Overtime"].values[0]
    t2_overtime = t2_idx["Overtime"].values[0]
    
    
    # time since last coach
    t1_coaches = allCoaches[allCoaches["Team"] == getKickerTeamName(team1)].sort_values("von")
    t2_coaches = allCoaches[allCoaches["Team"] == getKickerTeamName(team2)].sort_values("von")
    # time difference in days between game and last coach recruiting
    t1_coach_diff = (pendulum_time - pendulum.instance(t1_coaches.iloc[-1]["von"], tz='Europe/Berlin')).in_days()
    t2_coach_diff = (pendulum_time - pendulum.instance(t2_coaches.iloc[-1]["von"], tz='Europe/Berlin')).in_days()
    
    
    # Get last 5 games as list
    l5Games1 = []
    l5Games2 = []
    for g in range(1,6):
        t1_idx = lf_df1_reidx[lf_df1_reidx["Termin"] == row["Termin"]] - g
        l5Games1.append(t1_idx["Score"].values[0])
        t2_idx = lf_df1_reidx[lf_df1_reidx["Termin"] == row["Termin"]] - g 
        l5Games2.append(t2_idx["Score"].values[0])        
    
    
    # Get last 3 direct games between both teams (manually account for teams that havent met 3 times, set 0:0 default)
    last_direct_df = lf_df1_reidx[ (lf_df1_reidx["Gegner"] == team2) ]
    
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
    table_entry1 = allTables[ (allTables["Team"] == team1) & (allTables["Season"] == date_season) & (allTables["GameDay"] == gameDay) ]
    table_entry2 = allTables[ (allTables["Team"] == team2) & (allTables["Season"] == date_season) & (allTables["GameDay"] == gameDay) ]
    
    # last seasons last gameday entry
    ls_table_entry1 = allTables[ (allTables["Team"] == team1) & (allTables["Season"] == date_season-1) & (allTables["GameDay"] == 34) ]
    ls_table_entry2 = allTables[ (allTables["Team"] == team2) & (allTables["Season"] == date_season-1) & (allTables["GameDay"] == 34) ]
    
    # last 5 seasons league, 1 if all same as current league, 0 if at least one season was different
    t1_last5, t2_last5 = 0,0
    if (len(set(getPastLeagues(team1, date_season))) == 1) & (getPastLeagues(team1, date_season)[0]==table_entry1["League"].values[0]):
        t1_last5 = 1
    if (len(set(getPastLeagues(team2, date_season))) == 1) & (getPastLeagues(team2, date_season)[0]==table_entry2["League"].values[0]):
        t2_last5 = 1
    
    
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
    outDF = outDF.append({"Team1" : team1, 
                         "Team2" : team2, 
                         
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
                                                      
                         "LastSeasonPosition1" : ls_table_entry1["rank"].values[0], # last season's final position in league Team 1
                         "LastSeasonPosition2" : ls_table_entry2["rank"].values[0], # last season's final position in league Team 2
                         
                         "LastSeasonLeague1" : ls_table_entry1["League"].values[0], # last season league of Team 1
                         "LastSeasonLeague2" : ls_table_entry2["League"].values[0], # last season league of Team 2
                         
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
    
    
    
    
    
    