#imports
import requests
import numpy as np
import pandas as pd
from pandas import json_normalize
import time
from scipy.stats import poisson
from google.colab import auth
from google.auth import default
import gspread
# Instalar bibliotecas necesarias
!pip install gspread gspread_formatting


from gspread_formatting import *

auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)

#FUNCTION

def matchDayPLStats(matchday):


  uri = 'https://api.football-data.org/v4/competitions/PL/standings'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['standings'])
  df2 = json_normalize(df['table'])
  ##Get a list of PL teams IDs
  columns = list(df2)
  ids = []
  for i in columns:
    l = []
    l.append(df2[i][0]['team.id'])
    l.append(df2[i][0]['team.name'])
    ids.append(l)
#All matches stats for every team in PL. Execution time 2.3 min
#Output (id,goalsScoredHomeAv,goalsConcededHomeAv,goalsScoredAwayAv,goalsConcededAwayAv)
  leagueData=[]
  for i in ids:
    id=i[0]
    teamList=[]
    uri = 'https://api.football-data.org/v4/teams/'+str(id)+'/matches'
    headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

    response = requests.get(uri, headers=headers)
    data = response.json()
    df = json_normalize(data['matches'])

    teamdata = df[['matchday','competition.name','season.currentMatchday',
        'homeTeam.id','homeTeam.name',
        'homeTeam.tla','awayTeam.id','awayTeam.name',
          'awayTeam.tla', 'score.fullTime.home', 'score.fullTime.away']]


    teamDataH = teamdata[teamdata['competition.name']=='Premier League']
    teamDataA = teamdata[teamdata['competition.name']=='Premier League']
    teamHomeData = teamDataH[teamDataH['homeTeam.id']==id]
    teamHomeData = teamHomeData[teamHomeData['matchday']<matchday]
    teamAwayData = teamDataA[teamDataA['awayTeam.id']==id]
    teamAwayData = teamAwayData[teamAwayData['matchday']<matchday]

  #Goals conceded and scored Away/home
    goalsConcededAway = teamAwayData['score.fullTime.home'].sum()
    goalsScoredAway = teamAwayData['score.fullTime.away'].sum()

    goalsConcededHome = teamHomeData['score.fullTime.away'].sum()
    goalsScoredHome = teamHomeData['score.fullTime.home'].sum()

  #Average goals conceded and scored Away/home

    goalsConcededAwayAv = (teamAwayData['score.fullTime.home'].sum())/matchday
    goalsScoredAwayAv = teamAwayData['score.fullTime.away'].sum()/matchday

    goalsConcededHomeAv = teamHomeData['score.fullTime.away'].sum()/matchday
    goalsScoredHomeAv = teamHomeData['score.fullTime.home'].sum()/matchday

    teamList.append(id)
    teamList.append(goalsScoredHomeAv)
    teamList.append(goalsConcededHomeAv)
    teamList.append(goalsScoredAwayAv)
    teamList.append(goalsConcededAwayAv)
    leagueData.append(teamList)
    time.sleep(7)

    #Next step is to calculate PL average of goals conceded and scored by Local and Away teams
  uri = 'https://api.football-data.org/v4/competitions/PL/matches'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['matches'])

  nextGames = df[df['matchday']==matchday]
  # Total of goals conceded and scored for all PL teams, Average included.
  allTeamsGoals = df[df['matchday']<=matchday]
  allTeamsGoals= allTeamsGoals[['homeTeam.name','score.fullTime.home','score.fullTime.away','awayTeam.name','matchday']]


  allTeamsAwayGoalsScored = allTeamsGoals['score.fullTime.away'].sum()

  allTeamsHomeGoalsScored = allTeamsGoals['score.fullTime.home'].sum()


  allTeamsAwayGoalsScoredAv = (allTeamsGoals['score.fullTime.away'].sum()/20)/matchday

  allTeamsHomeGoalsScoredAv = (allTeamsGoals['score.fullTime.home'].sum()/20)/matchday
  #Next match day games by id
  nextGames= nextGames[['homeTeam.id','awayTeam.id','score.fullTime.home','score.fullTime.away','homeTeam.name','awayTeam.name']]

  homeTeams = nextGames['homeTeam.id'].tolist()
  awayTeams = nextGames['awayTeam.id'].tolist()
  homeGoals = nextGames['score.fullTime.home'].tolist()
  awayGoals = nextGames['score.fullTime.away'].tolist()
  homeTeamsNames = nextGames['homeTeam.name'].tolist()
  awayTeamsNames = nextGames['awayTeam.name'].tolist()
  #DF with ['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'] info

  leagueDatadf = pd.DataFrame(np.array(leagueData),
                   columns=['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'])
  leagueDatadf=leagueDatadf.set_index('id')

  #Next fixture data

  #Home team attack
  # attackPower: goalsScoredHomeAv, relativeAttackPower: goalsScoredHomeAv/allTeamsHomeGoalsScoredAv
  #Away team defense
  # defensePower: goalsConcededAwayAv, relativeDefensePower: goalsConcededAwayAv/allTeamsAwayGoalsScoredAv
  #Home team expected goals (heG): relativeAttackPower * relativeDefensePower * allTeamsHomeGoalsScoredAv

  #Away team attack
  # attackPower: goalsScoredAwayAv, relativeAttackPower: goalsScoredAwayAv/allTeamsAwayGoalsScoredAv
  #Home team defense
  # defensePower: goalsConcededHomeAv, relativeDefensePower: goalsConcededHomeAv/allTeamsAwayGoalsScoredAv
  #Away team expected goals(aeG): relativeAttackPower * relativeDefensePower * allTeamsAwayGoalsScoredAv

  #Probability of scoring X goals for home team -> poisson.pmf(k=X, mu=eG)
  #Probability of scoring more than X goals for home team -> poisson.cdf(k=X, mu=eG)

  #heG = goalsScoredHomeAv/allTeamsHomeGoalsScoredAv * goalsConcededAwayAv/allTeamsAwayGoalsScoredAv * allTeamsHomeGoalsScoredAv
  #aeG = goalsScoredAwayAv/allTeamsAwayGoalsScoredAv * goalsConcededHomeAv/allTeamsAwayGoalsScoredAv * allTeamsAwayGoalsScoredAv
  gameData=[]

  for i in range(0,len(homeTeams)):
      ls=[]
      idH = homeTeams[i]
      idA = awayTeams[i]
      hG = homeGoals[i]
      aG = awayGoals[i]
      nameHome = homeTeamsNames[i]
      nameAway = awayTeamsNames[i]
      heG = leagueDatadf.loc[idH].goalsScoredHomeAv/allTeamsHomeGoalsScoredAv * leagueDatadf.loc[idA].goalsConcededAwayAv/allTeamsAwayGoalsScoredAv * allTeamsHomeGoalsScoredAv
      aeG = leagueDatadf.loc[idA].goalsScoredAwayAv/allTeamsAwayGoalsScoredAv * leagueDatadf.loc[idH].goalsConcededHomeAv/allTeamsAwayGoalsScoredAv * allTeamsAwayGoalsScoredAv


      ph0 = 1 - poisson.cdf(k=0, mu=heG)
      ph1 = 1 - poisson.cdf(k=1, mu=heG)
      ph2 = 1 - poisson.cdf(k=2, mu=heG)
      pa0 = 1 - poisson.cdf(k=0, mu=aeG)
      pa1 = 1 - poisson.cdf(k=1, mu=aeG)
      pa2 = 1 - poisson.cdf(k=2, mu=aeG)
      #pt1 = pa1 * ph1
      #pt2 = pa2 * ph2




      ls.append(idH)
      ls.append(idA)
      ls.append(nameHome)
      ls.append(nameAway)
      ls.append(heG)
      ls.append(aeG)
      ls.append(ph0)
      ls.append(ph1)
      ls.append(ph2)
      ls.append(pa0)
      ls.append(pa1)
      ls.append(pa2)
      ls.append(hG)
      ls.append(aG)


      gameData.append(ls)




  gameDatadf = pd.DataFrame(np.array(gameData),
                    columns=['Home team id', 'Away team id','Home team', 'Away team','heg','aeg','+0 HG','+1 HG','+2 HG','+0 AG','+1 AG','+2 AG','HG','AG'])
  return gameDatadf





  #FUNCTION

def matchDayBLStats(matchday):


  uri = 'https://api.football-data.org/v4/competitions/BL1/standings'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['standings'])
  df2 = json_normalize(df['table'])
  ##Get a list of PL teams IDs
  columns = list(df2)
  ids = []
  for i in columns:
    l = []
    l.append(df2[i][0]['team.id'])
    l.append(df2[i][0]['team.name'])
    ids.append(l)
#All matches stats for every team in PL. Execution time 2.3 min
#Output (id,goalsScoredHomeAv,goalsConcededHomeAv,goalsScoredAwayAv,goalsConcededAwayAv)
  leagueData=[]
  for i in ids:
    id=i[0]
    teamList=[]
    uri = 'https://api.football-data.org/v4/teams/'+str(id)+'/matches'
    headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

    response = requests.get(uri, headers=headers)
    data = response.json()
    df = json_normalize(data['matches'])

    teamdata = df[['matchday','competition.name','season.currentMatchday',
        'homeTeam.id','homeTeam.name',
        'homeTeam.tla','awayTeam.id','awayTeam.name',
          'awayTeam.tla', 'score.fullTime.home', 'score.fullTime.away']]


    teamDataH = teamdata[teamdata['competition.name']=='Bundesliga']
    teamDataA = teamdata[teamdata['competition.name']=='Bundesliga']
    teamHomeData = teamDataH[teamDataH['homeTeam.id']==id]
    teamHomeData = teamHomeData[teamHomeData['matchday']<matchday]
    teamAwayData = teamDataA[teamDataA['awayTeam.id']==id]
    teamAwayData = teamAwayData[teamAwayData['matchday']<matchday]

  #Goals conceded and scored Away/home
    goalsConcededAway = teamAwayData['score.fullTime.home'].sum()
    goalsScoredAway = teamAwayData['score.fullTime.away'].sum()

    goalsConcededHome = teamHomeData['score.fullTime.away'].sum()
    goalsScoredHome = teamHomeData['score.fullTime.home'].sum()

  #Average goals conceded and scored Away/home

    goalsConcededAwayAv = (teamAwayData['score.fullTime.home'].sum())/matchday
    goalsScoredAwayAv = teamAwayData['score.fullTime.away'].sum()/matchday

    goalsConcededHomeAv = teamHomeData['score.fullTime.away'].sum()/matchday
    goalsScoredHomeAv = teamHomeData['score.fullTime.home'].sum()/matchday

    teamList.append(id)
    teamList.append(goalsScoredHomeAv)
    teamList.append(goalsConcededHomeAv)
    teamList.append(goalsScoredAwayAv)
    teamList.append(goalsConcededAwayAv)
    leagueData.append(teamList)
    time.sleep(7)

    #Next step is to calculate PL average of goals conceded and scored by Local and Away teams
  uri = 'https://api.football-data.org/v4/competitions/BL1/matches'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['matches'])

  nextGames = df[df['matchday']==matchday]
  # Total of goals conceded and scored for all PL teams, Average included.
  allTeamsGoals = df[df['matchday']<=matchday]
  allTeamsGoals= allTeamsGoals[['homeTeam.name','score.fullTime.home','score.fullTime.away','awayTeam.name','matchday']]


  allTeamsAwayGoalsScored = allTeamsGoals['score.fullTime.away'].sum()

  allTeamsHomeGoalsScored = allTeamsGoals['score.fullTime.home'].sum()


  allTeamsAwayGoalsScoredAv = (allTeamsGoals['score.fullTime.away'].sum()/18)/matchday

  allTeamsHomeGoalsScoredAv = (allTeamsGoals['score.fullTime.home'].sum()/18)/matchday
  #Next match day games by id
  nextGames= nextGames[['homeTeam.id','awayTeam.id','score.fullTime.home','score.fullTime.away','homeTeam.name','awayTeam.name']]

  homeTeams = nextGames['homeTeam.id'].tolist()
  awayTeams = nextGames['awayTeam.id'].tolist()
  homeGoals = nextGames['score.fullTime.home'].tolist()
  awayGoals = nextGames['score.fullTime.away'].tolist()
  homeTeamsNames = nextGames['homeTeam.name'].tolist()
  awayTeamsNames = nextGames['awayTeam.name'].tolist()
  #DF with ['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'] info

  leagueDatadf = pd.DataFrame(np.array(leagueData),
                   columns=['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'])
  leagueDatadf=leagueDatadf.set_index('id')

  gameData=[]

  for i in range(0,len(homeTeams)):
      ls=[]
      idH = homeTeams[i]
      idA = awayTeams[i]
      hG = homeGoals[i]
      aG = awayGoals[i]
      nameHome = homeTeamsNames[i]
      nameAway = awayTeamsNames[i]
      heG = leagueDatadf.loc[idH].goalsScoredHomeAv/allTeamsHomeGoalsScoredAv * leagueDatadf.loc[idA].goalsConcededAwayAv/allTeamsAwayGoalsScoredAv * allTeamsHomeGoalsScoredAv
      aeG = leagueDatadf.loc[idA].goalsScoredAwayAv/allTeamsAwayGoalsScoredAv * leagueDatadf.loc[idH].goalsConcededHomeAv/allTeamsAwayGoalsScoredAv * allTeamsAwayGoalsScoredAv


      ph0 = 1 - poisson.cdf(k=0, mu=heG)
      ph1 = 1 - poisson.cdf(k=1, mu=heG)
      ph2 = 1 - poisson.cdf(k=2, mu=heG)
      pa0 = 1 - poisson.cdf(k=0, mu=aeG)
      pa1 = 1 - poisson.cdf(k=1, mu=aeG)
      pa2 = 1 - poisson.cdf(k=2, mu=aeG)
      #pt1 = pa1 * ph1
      #pt2 = pa2 * ph2




      ls.append(idH)
      ls.append(idA)
      ls.append(nameHome)
      ls.append(nameAway)
      ls.append(heG)
      ls.append(aeG)
      ls.append(ph0)
      ls.append(ph1)
      ls.append(ph2)
      ls.append(pa0)
      ls.append(pa1)
      ls.append(pa2)
      ls.append(hG)
      ls.append(aG)


      gameData.append(ls)




  gameDatadf = pd.DataFrame(np.array(gameData),
                    columns=['Home team id', 'Away team id','Home team', 'Away team','heg','aeg','+0 HG','+1 HG','+2 HG','+0 AG','+1 AG','+2 AG','HG','AG'])
  return gameDatadf






  #FUNCTION

def matchDayPDStats(matchday):


  uri = 'https://api.football-data.org/v4/competitions/PD/standings'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['standings'])
  df2 = json_normalize(df['table'])
  ##Get a list of PL teams IDs
  columns = list(df2)
  ids = []
  for i in columns:
    l = []
    l.append(df2[i][0]['team.id'])
    l.append(df2[i][0]['team.name'])
    ids.append(l)
#All matches stats for every team in PL. Execution time 2.3 min
#Output (id,goalsScoredHomeAv,goalsConcededHomeAv,goalsScoredAwayAv,goalsConcededAwayAv)
  leagueData=[]
  for i in ids:
    id=i[0]
    teamList=[]
    uri = 'https://api.football-data.org/v4/teams/'+str(id)+'/matches'
    headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

    response = requests.get(uri, headers=headers)
    data = response.json()
    df = json_normalize(data['matches'])

    teamdata = df[['matchday','competition.name','season.currentMatchday',
        'homeTeam.id','homeTeam.name',
        'homeTeam.tla','awayTeam.id','awayTeam.name',
          'awayTeam.tla', 'score.fullTime.home', 'score.fullTime.away']]


    teamDataH = teamdata[teamdata['competition.name']=='Primera Division']
    teamDataA = teamdata[teamdata['competition.name']=='Primera Division']
    teamHomeData = teamDataH[teamDataH['homeTeam.id']==id]
    teamHomeData = teamHomeData[teamHomeData['matchday']<matchday]
    teamAwayData = teamDataA[teamDataA['awayTeam.id']==id]
    teamAwayData = teamAwayData[teamAwayData['matchday']<matchday]

  #Goals conceded and scored Away/home
    goalsConcededAway = teamAwayData['score.fullTime.home'].sum()
    goalsScoredAway = teamAwayData['score.fullTime.away'].sum()

    goalsConcededHome = teamHomeData['score.fullTime.away'].sum()
    goalsScoredHome = teamHomeData['score.fullTime.home'].sum()

  #Average goals conceded and scored Away/home

    goalsConcededAwayAv = (teamAwayData['score.fullTime.home'].sum())/matchday
    goalsScoredAwayAv = teamAwayData['score.fullTime.away'].sum()/matchday

    goalsConcededHomeAv = teamHomeData['score.fullTime.away'].sum()/matchday
    goalsScoredHomeAv = teamHomeData['score.fullTime.home'].sum()/matchday

    teamList.append(id)
    teamList.append(goalsScoredHomeAv)
    teamList.append(goalsConcededHomeAv)
    teamList.append(goalsScoredAwayAv)
    teamList.append(goalsConcededAwayAv)
    leagueData.append(teamList)
    time.sleep(7)

    #Next step is to calculate PL average of goals conceded and scored by Local and Away teams
  uri = 'https://api.football-data.org/v4/competitions/PD/matches'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['matches'])

  nextGames = df[df['matchday']==matchday]
  # Total of goals conceded and scored for all PL teams, Average included.
  allTeamsGoals = df[df['matchday']<=matchday]
  allTeamsGoals= allTeamsGoals[['homeTeam.name','score.fullTime.home','score.fullTime.away','awayTeam.name','matchday']]


  allTeamsAwayGoalsScored = allTeamsGoals['score.fullTime.away'].sum()

  allTeamsHomeGoalsScored = allTeamsGoals['score.fullTime.home'].sum()


  allTeamsAwayGoalsScoredAv = (allTeamsGoals['score.fullTime.away'].sum()/20)/matchday

  allTeamsHomeGoalsScoredAv = (allTeamsGoals['score.fullTime.home'].sum()/20)/matchday
  #Next match day games by id
  nextGames= nextGames[['homeTeam.id','awayTeam.id','score.fullTime.home','score.fullTime.away','homeTeam.name','awayTeam.name']]

  homeTeams = nextGames['homeTeam.id'].tolist()
  awayTeams = nextGames['awayTeam.id'].tolist()
  homeGoals = nextGames['score.fullTime.home'].tolist()
  awayGoals = nextGames['score.fullTime.away'].tolist()
  homeTeamsNames = nextGames['homeTeam.name'].tolist()
  awayTeamsNames = nextGames['awayTeam.name'].tolist()
  #DF with ['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'] info

  leagueDatadf = pd.DataFrame(np.array(leagueData),
                   columns=['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'])
  leagueDatadf=leagueDatadf.set_index('id')

  gameData=[]

  for i in range(0,len(homeTeams)):
      ls=[]
      idH = homeTeams[i]
      idA = awayTeams[i]
      hG = homeGoals[i]
      aG = awayGoals[i]
      nameHome = homeTeamsNames[i]
      nameAway = awayTeamsNames[i]
      heG = leagueDatadf.loc[idH].goalsScoredHomeAv/allTeamsHomeGoalsScoredAv * leagueDatadf.loc[idA].goalsConcededAwayAv/allTeamsAwayGoalsScoredAv * allTeamsHomeGoalsScoredAv
      aeG = leagueDatadf.loc[idA].goalsScoredAwayAv/allTeamsAwayGoalsScoredAv * leagueDatadf.loc[idH].goalsConcededHomeAv/allTeamsAwayGoalsScoredAv * allTeamsAwayGoalsScoredAv


      ph0 = 1 - poisson.cdf(k=0, mu=heG)
      ph1 = 1 - poisson.cdf(k=1, mu=heG)
      ph2 = 1 - poisson.cdf(k=2, mu=heG)
      pa0 = 1 - poisson.cdf(k=0, mu=aeG)
      pa1 = 1 - poisson.cdf(k=1, mu=aeG)
      pa2 = 1 - poisson.cdf(k=2, mu=aeG)
      #pt1 = pa1 * ph1
      #pt2 = pa2 * ph2




      ls.append(idH)
      ls.append(idA)
      ls.append(nameHome)
      ls.append(nameAway)
      ls.append(heG)
      ls.append(aeG)
      ls.append(ph0)
      ls.append(ph1)
      ls.append(ph2)
      ls.append(pa0)
      ls.append(pa1)
      ls.append(pa2)
      ls.append(hG)
      ls.append(aG)


      gameData.append(ls)




  gameDatadf = pd.DataFrame(np.array(gameData),
                    columns=['Home team id', 'Away team id','Home team', 'Away team','heg','aeg','+0 HG','+1 HG','+2 HG','+0 AG','+1 AG','+2 AG','HG','AG'])
  return gameDatadf




  #FUNCTION

def matchDaySAStats(matchday):


  uri = 'https://api.football-data.org/v4/competitions/SA/standings'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['standings'])
  df2 = json_normalize(df['table'])
  ##Get a list of PL teams IDs
  columns = list(df2)
  ids = []
  for i in columns:
    l = []
    l.append(df2[i][0]['team.id'])
    l.append(df2[i][0]['team.name'])
    ids.append(l)
#All matches stats for every team in PL. Execution time 2.3 min
#Output (id,goalsScoredHomeAv,goalsConcededHomeAv,goalsScoredAwayAv,goalsConcededAwayAv)
  leagueData=[]
  for i in ids:
    id=i[0]
    teamList=[]
    uri = 'https://api.football-data.org/v4/teams/'+str(id)+'/matches'
    headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

    response = requests.get(uri, headers=headers)
    data = response.json()
    df = json_normalize(data['matches'])

    teamdata = df[['matchday','competition.name','season.currentMatchday',
        'homeTeam.id','homeTeam.name',
        'homeTeam.tla','awayTeam.id','awayTeam.name',
          'awayTeam.tla', 'score.fullTime.home', 'score.fullTime.away']]


    teamDataH = teamdata[teamdata['competition.name']=='Serie A']
    teamDataA = teamdata[teamdata['competition.name']=='Serie A']
    teamHomeData = teamDataH[teamDataH['homeTeam.id']==id]
    teamHomeData = teamHomeData[teamHomeData['matchday']<matchday]
    teamAwayData = teamDataA[teamDataA['awayTeam.id']==id]
    teamAwayData = teamAwayData[teamAwayData['matchday']<matchday]

  #Goals conceded and scored Away/home
    goalsConcededAway = teamAwayData['score.fullTime.home'].sum()
    goalsScoredAway = teamAwayData['score.fullTime.away'].sum()

    goalsConcededHome = teamHomeData['score.fullTime.away'].sum()
    goalsScoredHome = teamHomeData['score.fullTime.home'].sum()

  #Average goals conceded and scored Away/home

    goalsConcededAwayAv = (teamAwayData['score.fullTime.home'].sum())/matchday
    goalsScoredAwayAv = teamAwayData['score.fullTime.away'].sum()/matchday

    goalsConcededHomeAv = teamHomeData['score.fullTime.away'].sum()/matchday
    goalsScoredHomeAv = teamHomeData['score.fullTime.home'].sum()/matchday

    teamList.append(id)
    teamList.append(goalsScoredHomeAv)
    teamList.append(goalsConcededHomeAv)
    teamList.append(goalsScoredAwayAv)
    teamList.append(goalsConcededAwayAv)
    leagueData.append(teamList)
    time.sleep(7)

    #Next step is to calculate PL average of goals conceded and scored by Local and Away teams
  uri = 'https://api.football-data.org/v4/competitions/SA/matches'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['matches'])

  nextGames = df[df['matchday']==matchday]
  # Total of goals conceded and scored for all PL teams, Average included.
  allTeamsGoals = df[df['matchday']<=matchday]
  allTeamsGoals= allTeamsGoals[['homeTeam.name','score.fullTime.home','score.fullTime.away','awayTeam.name','matchday']]


  allTeamsAwayGoalsScored = allTeamsGoals['score.fullTime.away'].sum()

  allTeamsHomeGoalsScored = allTeamsGoals['score.fullTime.home'].sum()


  allTeamsAwayGoalsScoredAv = (allTeamsGoals['score.fullTime.away'].sum()/20)/matchday

  allTeamsHomeGoalsScoredAv = (allTeamsGoals['score.fullTime.home'].sum()/20)/matchday
  #Next match day games by id
  nextGames= nextGames[['homeTeam.id','awayTeam.id','score.fullTime.home','score.fullTime.away','homeTeam.name','awayTeam.name']]

  homeTeams = nextGames['homeTeam.id'].tolist()
  awayTeams = nextGames['awayTeam.id'].tolist()
  homeGoals = nextGames['score.fullTime.home'].tolist()
  awayGoals = nextGames['score.fullTime.away'].tolist()
  homeTeamsNames = nextGames['homeTeam.name'].tolist()
  awayTeamsNames = nextGames['awayTeam.name'].tolist()
  #DF with ['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'] info

  leagueDatadf = pd.DataFrame(np.array(leagueData),
                   columns=['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'])
  leagueDatadf=leagueDatadf.set_index('id')



  for i in range(0,len(homeTeams)):
      ls=[]
      idH = homeTeams[i]
      idA = awayTeams[i]
      hG = homeGoals[i]
      aG = awayGoals[i]
      nameHome = homeTeamsNames[i]
      nameAway = awayTeamsNames[i]
      heG = leagueDatadf.loc[idH].goalsScoredHomeAv/allTeamsHomeGoalsScoredAv * leagueDatadf.loc[idA].goalsConcededAwayAv/allTeamsAwayGoalsScoredAv * allTeamsHomeGoalsScoredAv
      aeG = leagueDatadf.loc[idA].goalsScoredAwayAv/allTeamsAwayGoalsScoredAv * leagueDatadf.loc[idH].goalsConcededHomeAv/allTeamsAwayGoalsScoredAv * allTeamsAwayGoalsScoredAv


      ph0 = 1 - poisson.cdf(k=0, mu=heG)
      ph1 = 1 - poisson.cdf(k=1, mu=heG)
      ph2 = 1 - poisson.cdf(k=2, mu=heG)
      pa0 = 1 - poisson.cdf(k=0, mu=aeG)
      pa1 = 1 - poisson.cdf(k=1, mu=aeG)
      pa2 = 1 - poisson.cdf(k=2, mu=aeG)
      #pt1 = pa1 * ph1
      #pt2 = pa2 * ph2




      ls.append(idH)
      ls.append(idA)
      ls.append(nameHome)
      ls.append(nameAway)
      ls.append(heG)
      ls.append(aeG)
      ls.append(ph0)
      ls.append(ph1)
      ls.append(ph2)
      ls.append(pa0)
      ls.append(pa1)
      ls.append(pa2)
      ls.append(hG)
      ls.append(aG)


      gameData.append(ls)




  gameDatadf = pd.DataFrame(np.array(gameData),
                    columns=['Home team id', 'Away team id','Home team', 'Away team','heg','aeg','+0 HG','+1 HG','+2 HG','+0 AG','+1 AG','+2 AG','HG','AG'])
  return gameDatadf


  #FUNCTION

def matchDayL1Stats(matchday):


  uri = 'https://api.football-data.org/v4/competitions/FL1/standings'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['standings'])
  df2 = json_normalize(df['table'])
  ##Get a list of PL teams IDs
  columns = list(df2)
  ids = []
  for i in columns:
    l = []
    l.append(df2[i][0]['team.id'])
    l.append(df2[i][0]['team.name'])
    ids.append(l)
#All matches stats for every team in PL. Execution time 2.3 min
#Output (id,goalsScoredHomeAv,goalsConcededHomeAv,goalsScoredAwayAv,goalsConcededAwayAv)
  leagueData=[]
  for i in ids:
    id=i[0]
    teamList=[]
    uri = 'https://api.football-data.org/v4/teams/'+str(id)+'/matches'
    headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

    response = requests.get(uri, headers=headers)
    data = response.json()
    df = json_normalize(data['matches'])

    teamdata = df[['matchday','competition.name','season.currentMatchday',
        'homeTeam.id','homeTeam.name',
        'homeTeam.tla','awayTeam.id','awayTeam.name',
          'awayTeam.tla', 'score.fullTime.home', 'score.fullTime.away']]


    teamDataH = teamdata[teamdata['competition.name']=='Ligue 1']
    teamDataA = teamdata[teamdata['competition.name']=='Ligue 1']
    teamHomeData = teamDataH[teamDataH['homeTeam.id']==id]
    teamHomeData = teamHomeData[teamHomeData['matchday']<matchday]
    teamAwayData = teamDataA[teamDataA['awayTeam.id']==id]
    teamAwayData = teamAwayData[teamAwayData['matchday']<matchday]

  #Goals conceded and scored Away/home
    goalsConcededAway = teamAwayData['score.fullTime.home'].sum()
    goalsScoredAway = teamAwayData['score.fullTime.away'].sum()

    goalsConcededHome = teamHomeData['score.fullTime.away'].sum()
    goalsScoredHome = teamHomeData['score.fullTime.home'].sum()

  #Average goals conceded and scored Away/home

    goalsConcededAwayAv = (teamAwayData['score.fullTime.home'].sum())/matchday
    goalsScoredAwayAv = teamAwayData['score.fullTime.away'].sum()/matchday

    goalsConcededHomeAv = teamHomeData['score.fullTime.away'].sum()/matchday
    goalsScoredHomeAv = teamHomeData['score.fullTime.home'].sum()/matchday

    teamList.append(id)
    teamList.append(goalsScoredHomeAv)
    teamList.append(goalsConcededHomeAv)
    teamList.append(goalsScoredAwayAv)
    teamList.append(goalsConcededAwayAv)
    leagueData.append(teamList)
    time.sleep(7)

    #Next step is to calculate PL average of goals conceded and scored by Local and Away teams
  uri = 'https://api.football-data.org/v4/competitions/FL1/matches'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['matches'])

  nextGames = df[df['matchday']==matchday]
  # Total of goals conceded and scored for all PL teams, Average included.
  allTeamsGoals = df[df['matchday']<=matchday]
  allTeamsGoals= allTeamsGoals[['homeTeam.name','score.fullTime.home','score.fullTime.away','awayTeam.name','matchday']]


  allTeamsAwayGoalsScored = allTeamsGoals['score.fullTime.away'].sum()

  allTeamsHomeGoalsScored = allTeamsGoals['score.fullTime.home'].sum()


  allTeamsAwayGoalsScoredAv = (allTeamsGoals['score.fullTime.away'].sum()/18)/matchday

  allTeamsHomeGoalsScoredAv = (allTeamsGoals['score.fullTime.home'].sum()/18)/matchday
  #Next match day games by id
  nextGames= nextGames[['homeTeam.id','awayTeam.id','score.fullTime.home','score.fullTime.away','homeTeam.name','awayTeam.name']]

  homeTeams = nextGames['homeTeam.id'].tolist()
  awayTeams = nextGames['awayTeam.id'].tolist()
  homeGoals = nextGames['score.fullTime.home'].tolist()
  awayGoals = nextGames['score.fullTime.away'].tolist()
  homeTeamsNames = nextGames['homeTeam.name'].tolist()
  awayTeamsNames = nextGames['awayTeam.name'].tolist()
  #DF with ['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'] info

  leagueDatadf = pd.DataFrame(np.array(leagueData),
                   columns=['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'])
  leagueDatadf=leagueDatadf.set_index('id')


  gameData=[]

  for i in range(0,len(homeTeams)):
      ls=[]
      idH = homeTeams[i]
      idA = awayTeams[i]
      hG = homeGoals[i]
      aG = awayGoals[i]
      nameHome = homeTeamsNames[i]
      nameAway = awayTeamsNames[i]
      heG = leagueDatadf.loc[idH].goalsScoredHomeAv/allTeamsHomeGoalsScoredAv * leagueDatadf.loc[idA].goalsConcededAwayAv/allTeamsAwayGoalsScoredAv * allTeamsHomeGoalsScoredAv
      aeG = leagueDatadf.loc[idA].goalsScoredAwayAv/allTeamsAwayGoalsScoredAv * leagueDatadf.loc[idH].goalsConcededHomeAv/allTeamsAwayGoalsScoredAv * allTeamsAwayGoalsScoredAv


      ph0 = 1 - poisson.cdf(k=0, mu=heG)
      ph1 = 1 - poisson.cdf(k=1, mu=heG)
      ph2 = 1 - poisson.cdf(k=2, mu=heG)
      pa0 = 1 - poisson.cdf(k=0, mu=aeG)
      pa1 = 1 - poisson.cdf(k=1, mu=aeG)
      pa2 = 1 - poisson.cdf(k=2, mu=aeG)
      #pt1 = pa1 * ph1
      #pt2 = pa2 * ph2




      ls.append(idH)
      ls.append(idA)
      ls.append(nameHome)
      ls.append(nameAway)
      ls.append(heG)
      ls.append(aeG)
      ls.append(ph0)
      ls.append(ph1)
      ls.append(ph2)
      ls.append(pa0)
      ls.append(pa1)
      ls.append(pa2)
      ls.append(hG)
      ls.append(aG)


      gameData.append(ls)




  gameDatadf = pd.DataFrame(np.array(gameData),
                    columns=['Home team id', 'Away team id','Home team', 'Away team','heg','aeg','+0 HG','+1 HG','+2 HG','+0 AG','+1 AG','+2 AG','HG','AG'])
  return gameDatadf





def matchDayPL2Stats(matchday):


  uri = 'https://api.football-data.org/v4/competitions/ELC/standings'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['standings'])
  df2 = json_normalize(df['table'])
  ##Get a list of PL teams IDs
  columns = list(df2)
  ids = []
  for i in columns:
    l = []
    l.append(df2[i][0]['team.id'])
    l.append(df2[i][0]['team.name'])
    ids.append(l)
#All matches stats for every team in PL. Execution time 2.3 min
#Output (id,goalsScoredHomeAv,goalsConcededHomeAv,goalsScoredAwayAv,goalsConcededAwayAv)
  leagueData=[]
  for i in ids:
    id=i[0]
    teamList=[]
    uri = 'https://api.football-data.org/v4/teams/'+str(id)+'/matches'
    headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

    response = requests.get(uri, headers=headers)
    data = response.json()
    df = json_normalize(data['matches'])

    teamdata = df[['matchday','competition.name','season.currentMatchday',
        'homeTeam.id','homeTeam.name',
        'homeTeam.tla','awayTeam.id','awayTeam.name',
          'awayTeam.tla', 'score.fullTime.home', 'score.fullTime.away']]


    teamDataH = teamdata[teamdata['competition.name']=='Championship']
    teamDataA = teamdata[teamdata['competition.name']=='Championship']
    teamHomeData = teamDataH[teamDataH['homeTeam.id']==id]
    teamHomeData = teamHomeData[teamHomeData['matchday']<matchday]
    teamAwayData = teamDataA[teamDataA['awayTeam.id']==id]
    teamAwayData = teamAwayData[teamAwayData['matchday']<matchday]

  #Goals conceded and scored Away/home
    goalsConcededAway = teamAwayData['score.fullTime.home'].sum()
    goalsScoredAway = teamAwayData['score.fullTime.away'].sum()

    goalsConcededHome = teamHomeData['score.fullTime.away'].sum()
    goalsScoredHome = teamHomeData['score.fullTime.home'].sum()

  #Average goals conceded and scored Away/home

    goalsConcededAwayAv = (teamAwayData['score.fullTime.home'].sum())/matchday
    goalsScoredAwayAv = teamAwayData['score.fullTime.away'].sum()/matchday

    goalsConcededHomeAv = teamHomeData['score.fullTime.away'].sum()/matchday
    goalsScoredHomeAv = teamHomeData['score.fullTime.home'].sum()/matchday

    teamList.append(id)
    teamList.append(goalsScoredHomeAv)
    teamList.append(goalsConcededHomeAv)
    teamList.append(goalsScoredAwayAv)
    teamList.append(goalsConcededAwayAv)
    leagueData.append(teamList)
    time.sleep(7)

    #Next step is to calculate PL average of goals conceded and scored by Local and Away teams
  uri = 'https://api.football-data.org/v4/competitions/ELC/matches'
  headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }

  response = requests.get(uri, headers=headers)
  data = response.json()
  df = json_normalize(data['matches'])

  nextGames = df[df['matchday']==matchday]
  # Total of goals conceded and scored for all PL teams, Average included.
  allTeamsGoals = df[df['matchday']<=matchday]
  allTeamsGoals= allTeamsGoals[['homeTeam.name','score.fullTime.home','score.fullTime.away','awayTeam.name','matchday']]


  allTeamsAwayGoalsScored = allTeamsGoals['score.fullTime.away'].sum()

  allTeamsHomeGoalsScored = allTeamsGoals['score.fullTime.home'].sum()


  allTeamsAwayGoalsScoredAv = (allTeamsGoals['score.fullTime.away'].sum()/24)/matchday

  allTeamsHomeGoalsScoredAv = (allTeamsGoals['score.fullTime.home'].sum()/24)/matchday
  #Next match day games by id
  nextGames= nextGames[['homeTeam.id','awayTeam.id','score.fullTime.home','score.fullTime.away','homeTeam.name','awayTeam.name']]

  homeTeams = nextGames['homeTeam.id'].tolist()
  awayTeams = nextGames['awayTeam.id'].tolist()
  homeGoals = nextGames['score.fullTime.home'].tolist()
  awayGoals = nextGames['score.fullTime.away'].tolist()
  homeTeamsNames = nextGames['homeTeam.name'].tolist()
  awayTeamsNames = nextGames['awayTeam.name'].tolist()
  #DF with ['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'] info

  leagueDatadf = pd.DataFrame(np.array(leagueData),
                   columns=['id','goalsScoredHomeAv', 'goalsConcededHomeAv','goalsScoredAwayAv','goalsConcededAwayAv'])
  leagueDatadf=leagueDatadf.set_index('id')

  #Next fixture data

  #Home team attack
  # attackPower: goalsScoredHomeAv, relativeAttackPower: goalsScoredHomeAv/allTeamsHomeGoalsScoredAv
  #Away team defense
  # defensePower: goalsConcededAwayAv, relativeDefensePower: goalsConcededAwayAv/allTeamsAwayGoalsScoredAv
  #Home team expected goals (heG): relativeAttackPower * relativeDefensePower * allTeamsHomeGoalsScoredAv

  #Away team attack
  # attackPower: goalsScoredAwayAv, relativeAttackPower: goalsScoredAwayAv/allTeamsAwayGoalsScoredAv
  #Home team defense
  # defensePower: goalsConcededHomeAv, relativeDefensePower: goalsConcededHomeAv/allTeamsAwayGoalsScoredAv
  #Away team expected goals(aeG): relativeAttackPower * relativeDefensePower * allTeamsAwayGoalsScoredAv

  #Probability of scoring X goals for home team -> poisson.pmf(k=X, mu=eG)
  #Probability of scoring more than X goals for home team -> poisson.cdf(k=X, mu=eG)

  #heG = goalsScoredHomeAv/allTeamsHomeGoalsScoredAv * goalsConcededAwayAv/allTeamsAwayGoalsScoredAv * allTeamsHomeGoalsScoredAv
  #aeG = goalsScoredAwayAv/allTeamsAwayGoalsScoredAv * goalsConcededHomeAv/allTeamsAwayGoalsScoredAv * allTeamsAwayGoalsScoredAv
  gameData=[]

  for i in range(0,len(homeTeams)):
      ls=[]
      idH = homeTeams[i]
      idA = awayTeams[i]
      hG = homeGoals[i]
      aG = awayGoals[i]
      nameHome = homeTeamsNames[i]
      nameAway = awayTeamsNames[i]
      heG = leagueDatadf.loc[idH].goalsScoredHomeAv/allTeamsHomeGoalsScoredAv * leagueDatadf.loc[idA].goalsConcededAwayAv/allTeamsAwayGoalsScoredAv * allTeamsHomeGoalsScoredAv
      aeG = leagueDatadf.loc[idA].goalsScoredAwayAv/allTeamsAwayGoalsScoredAv * leagueDatadf.loc[idH].goalsConcededHomeAv/allTeamsAwayGoalsScoredAv * allTeamsAwayGoalsScoredAv


      ph0 = 1 - poisson.cdf(k=0, mu=heG)
      ph1 = 1 - poisson.cdf(k=1, mu=heG)
      ph2 = 1 - poisson.cdf(k=2, mu=heG)
      pa0 = 1 - poisson.cdf(k=0, mu=aeG)
      pa1 = 1 - poisson.cdf(k=1, mu=aeG)
      pa2 = 1 - poisson.cdf(k=2, mu=aeG)
      #pt1 = pa1 * ph1
      #pt2 = pa2 * ph2




      ls.append(idH)
      ls.append(idA)
      ls.append(nameHome)
      ls.append(nameAway)
      ls.append(heG)
      ls.append(aeG)
      ls.append(ph0)
      ls.append(ph1)
      ls.append(ph2)
      ls.append(pa0)
      ls.append(pa1)
      ls.append(pa2)
      ls.append(hG)
      ls.append(aG)


      gameData.append(ls)




  gameDatadf = pd.DataFrame(np.array(gameData),
                    columns=['Home team id', 'Away team id','Home team', 'Away team','heg','aeg','+0 HG','+1 HG','+2 HG','+0 AG','+1 AG','+2 AG','HG','AG'])
  return gameDatadf

  def toHistL1(numjornada1,numjornada2,nombreHist):

 for i in range(numjornada1, numjornada2):
    worksheet = gc.open(nombreHist)
    sheet_name = f"Sheet{i}"  # Generate sheet name based on loop index

    # Check if the sheet already exists
    if sheet_name not in [s.title for s in worksheet.worksheets()]:
        worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        df = matchDayL1Stats(i)

# Create a new sheet

        sheet = worksheet.worksheet(sheet_name)  # Get the sheet (either existing or newly created)
        sheet.update([df.columns.values.tolist()] + df.fillna(-1).values.tolist())


def toHistBL(numjornada1,numjornada2,nombreHist):

 for i in range(numjornada1, numjornada2):

    worksheet = gc.open(nombreHist)
    sheet_name = f"Sheet{i}"  # Generate sheet name based on loop index

    # Check if the sheet already exists
    if sheet_name not in [s.title for s in worksheet.worksheets()]:
        worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        df = matchDayBLStats(i) # Create a new sheet

        sheet = worksheet.worksheet(sheet_name)  # Get the sheet (either existing or newly created)
        sheet.update([df.columns.values.tolist()] + df.fillna(-1).values.tolist())


def toHistPD(numjornada1,numjornada2,nombreHist):

 for i in range(numjornada1, numjornada2):
    worksheet = gc.open(nombreHist)
    sheet_name = f"Sheet{i}"  # Generate sheet name based on loop index

    # Check if the sheet already exists
    if sheet_name not in [s.title for s in worksheet.worksheets()]:
        worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        df = matchDayPDStats(i)
  # Create a new sheet

        sheet = worksheet.worksheet(sheet_name)  # Get the sheet (either existing or newly created)
        sheet.update([df.columns.values.tolist()] + df.fillna(-1).values.tolist())


def toHistPL(numjornada1,numjornada2,nombreHist):

 for i in range(numjornada1, numjornada2):
    worksheet = gc.open(nombreHist)
    sheet_name = f"Sheet{i}"  # Generate sheet name based on loop index

    # Check if the sheet already exists
    if sheet_name not in [s.title for s in worksheet.worksheets()]:
        worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        df = matchDayPLStats(i)  # Create a new sheet

        sheet = worksheet.worksheet(sheet_name)  # Get the sheet (either existing or newly created)
        sheet.update([df.columns.values.tolist()] + df.fillna(-1).values.tolist())


def toHistPL2(numjornada1,numjornada2,nombreHist):

 for i in range(numjornada1, numjornada2):
    worksheet = gc.open(nombreHist)
    sheet_name = f"Sheet{i}"  # Generate sheet name based on loop index

    # Check if the sheet already exists
    if sheet_name not in [s.title for s in worksheet.worksheets()]:
        worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        df = matchDayPL2Stats(i)  # Create a new sheet

        sheet = worksheet.worksheet(sheet_name)  # Get the sheet (either existing or newly created)
        sheet.update([df.columns.values.tolist()] + df.fillna(-1).values.tolist())





def toHistSA(numjornada1,numjornada2,nombreHist):

 for i in range(numjornada1, numjornada2):

    worksheet = gc.open(nombreHist)
    sheet_name = f"Sheet{i}"  # Generate sheet name based on loop index

    # Check if the sheet already exists
    if sheet_name not in [s.title for s in worksheet.worksheets()]:
        worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
        df = matchDaySAStats(i)

        sheet = worksheet.worksheet(sheet_name)  # Get the sheet (either existing or newly created)
        sheet.update([df.columns.values.tolist()] + df.fillna(-1).values.tolist())

        import requests
from datetime import datetime

# API key and base URL
API_KEY = 'babfc8832fa344beb6f5398516d66ffe'  # Replace with your actual API key
BASE_URL = 'https://api.football-data.org/v4/competitions/'

headers = { 'X-Auth-Token': 'babfc8832fa344beb6f5398516d66ffe', 'Accept-Encoding': '' }


def get_next_gameweek_number(competition_code):

    url = f"{BASE_URL}/{competition_code}/matches"  # Corrected URL structure
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        print("Error:", response.json())
        return None

    data = response.json()
    matches = data.get('matches', [])

    # Get current date
    now = datetime.now()

    for match in matches:
        match_date = datetime.fromisoformat(match['utcDate'][:-1])

        # Check if the match is in the future
        if match_date > now:
            # Return the first upcoming matchday (gameweek) number
            gameweek_number = match['matchday']
            print(f"Next Gameweek Number for {competition_code}: {gameweek_number}")
            return None

    print(f"No upcoming gameweeks found for {competition_code}.")
    time.sleep(3)
    return None

# Run the function for Premier League (PL) and La Liga (PD)
get_next_gameweek_number('BL1') # Bundesliga
get_next_gameweek_number('FL1') # Ligue1
get_next_gameweek_number('PD')  # La Liga
get_next_gameweek_number('SA')  # Serie A
get_next_gameweek_number('PL')  # Premier League
get_next_gameweek_number('ELC') # Championship




def resultsFromHist(
    nombre_google_sheet_games: str,
    nombre_hoja_games: str,
    nombre_google_sheet_hist: str,
    nombre_hoja_hist: str
):
    """
    Función que suma los valores de las columnas M y N de una hoja de Google Sheets ("Games")
    y guarda el resultado en la columna K de otra hoja de Google Sheets ("HIST").

    Parámetros:
    - nombre_google_sheet_games: Nombre de la hoja de cálculo "Games".
    - nombre_hoja_games: Nombre de la hoja interna en "Games".
    - nombre_google_sheet_hist: Nombre de la hoja de cálculo "HIST".
    - nombre_hoja_hist: Nombre de la hoja interna en "HIST".
    - ruta_credenciales: Ruta al archivo JSON de credenciales de Google Cloud.
    """


    # Abre las hojas de cálculo y las hojas internas
    try:
        games_sheet = gc.open(nombre_google_sheet_games).worksheet(nombre_hoja_games)
        hist_sheet = gc.open(nombre_google_sheet_hist).worksheet(nombre_hoja_hist)
    except Exception as e:
        print(f"Error openning sheets: {e}")
        return

    # Obtén los valores de las columnas M y N de "Games"
    column_m = games_sheet.col_values(13)  # Columna M (índice 13)
    column_n = games_sheet.col_values(14)  # Columna N (índice 14)

    # Itera sobre las filas, suma los valores y escribe en "HIST"
    for i in range(1, len(column_m)):  # Comienza desde 1 para omitir el encabezado
        try:
            value_m = float(column_m[i]) if column_m[i] else 0
            value_n = float(column_n[i]) if column_n[i] else 0
            suma = value_m + value_n
            hist_sheet.update_cell(i + 1, 11, suma)  # Columna K (índice 11)
        except Exception as e:
            print(f"Error in {i + 1}: {e}")

    print("Completed")

import pandas as pd

# Función para actualizar la hoja 'Summary'
def update_summary_sheet(sheet_name):
    # Abre la hoja de cálculo por nombre
    spreadsheet = gc.open(sheet_name)

    # Obtén todas las hojas excepto 'Summary'
    sheets = [sheet for sheet in spreadsheet.worksheets() if sheet.title != 'Summary']

    # Prepara una lista para almacenar los valores de la columna K (índice 10)
    k_values = []

    # Itera sobre cada hoja
    for sheet in sheets:
        # Obtén los datos de la hoja
        data = sheet.get_all_values()
        df = pd.DataFrame(data)

        # Verifica que la hoja tiene suficientes filas y columnas
        if df.shape[0] <= 1 or df.shape[1] <= 11:
            continue  # Si no hay suficientes datos, pasa a la siguiente hoja

        # Ignorar la primera fila (encabezados)
        df = df.iloc[1:].reset_index(drop=True)

        # Filtra las filas donde ambas columnas K (índice 10) y L (índice 11) no estén vacías
        filtered_df = df[(df[10].astype(str).str.strip() != '') & (df[11].astype(str).str.strip() != '')]

        # Añade los valores de la columna K (índice 10) a la lista
        k_values.extend(filtered_df[10].tolist())

    # Obtén la hoja 'Summary'
    summary_sheet = spreadsheet.worksheet('Summary')

    # Obtén los datos de la hoja 'Summary'
    summary_data = summary_sheet.get_all_values()

    # Si la hoja está vacía, inicializamos un DataFrame vacío con suficientes columnas
    if not summary_data:
        summary_df = pd.DataFrame(columns=[str(i) for i in range(12)])  # Crear 12 columnas como strings
    else:
        summary_df = pd.DataFrame(summary_data)

    # Verifica que 'Summary' tiene suficientes columnas
    if summary_df.shape[1] <= 11:
        print("La hoja 'Summary' no tiene suficientes columnas.")
        return

    # Asegurar que haya suficientes filas en summary_df
    total_rows_needed = len(k_values) + (len(k_values) // 3)  # Se agrega una fila vacía cada 3 valores
    while summary_df.shape[0] < total_rows_needed + 3:  # +3 porque empezamos en la fila 4
        summary_df.loc[len(summary_df)] = [''] * summary_df.shape[1]  # Añadir filas vacías si es necesario

    # Insertar los valores en la columna K, empezando desde la fila 4 con saltos cada 3 valores
    start_row = 2
    formatted_k_values = []
    count = 0

    for value in k_values:
        formatted_k_values.append([value])
        count += 1
        if count % 3 == 0:
            formatted_k_values.append([""])  # Insertar fila vacía cada 3 valores

    # Actualizar solo la columna K en la hoja 'Summary'
    summary_sheet.update(f'K{start_row}:K{start_row+len(formatted_k_values)-1}', formatted_k_values)


resultsFromHist("BLGames2425","Sheet30","HIST312425","Sheet1")
resultsFromHist("L1Games2425","Sheet30","HIST312425","Sheet2")
resultsFromHist("PDGames2425","Sheet32","HIST312425","Sheet3")
resultsFromHist("SAGames2425","Sheet33","HIST312425","Sheet5")
resultsFromHist("PLGames2425","Sheet33","HIST312425","Sheet4")
resultsFromHist("PL2Games2425","Sheet43","HIST312425","Sheet6")

