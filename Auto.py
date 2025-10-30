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


auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)




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



import gspread
from google.colab import auth
from oauth2client.client import GoogleCredentials
import pandas as pd
from googleapiclient.discovery import build

def get_sheet_id_by_name(sheet_name):
    """
    Obtiene la ID de un Google Sheet por su nombre.

    Args:
    - sheet_name: El nombre del Google Sheet.

    Returns:
    - La ID del Google Sheet.
    """
    # Autenticación y acceso a la API de Google Drive
    auth.authenticate_user()
    drive_service = build('drive', 'v3', credentials=GoogleCredentials.get_application_default())

    # Buscar el archivo por su nombre
    results = drive_service.files().list(q=f"name='{sheet_name}'").execute()
    items = results.get('files', [])

    # Obtener la ID del primer resultado (asumiendo que hay solo un archivo con ese nombre)
    if items:
        return items[0]['id']
    else:
        raise ValueError(f"No se encontró ningún archivo con el nombre '{sheet_name}'.")

def read_google_sheets_into_dataframe(sheet_names):
    """
    Lee todas las hojas de Google Sheets especificadas por sus nombres y las concatena en un solo DataFrame.

    Args:
    - sheet_names: Una lista de nombres de Google Sheets.

    Returns:
    - DataFrame que contiene los datos de todas las hojas concatenados.
    """
    # Autenticación y acceso a la API de Google Sheets
    auth.authenticate_user()

    # Inicializar una lista para almacenar los DataFrames de cada hoja
    dfs = []

    # Iterar sobre cada nombre de hoja
    for sheet_name in sheet_names:
        # Obtener la ID del Google Sheet por su nombre
        sheet_id = get_sheet_id_by_name(sheet_name)

        # Abrir el Google Sheet por su ID
        workbook = gc.open_by_key(sheet_id)

        # Iterar sobre cada hoja en el Google Sheet
        for worksheet in workbook.worksheets():
            # Leer los datos de la hoja actual y convertirlos en DataFrame
            data = worksheet.get_all_values()
            df = pd.DataFrame(data[1:], columns=data[0])  # Asumiendo que la primera fila son los encabezados
            dfs.append(df)

    # Concatenar todos los DataFrames en uno solo
    combined_df = pd.concat(dfs, ignore_index=True)

    return combined_df

# Ejemplo de uso
sheet_names = ['PLGames2425', 'SAGames2425', 'PDGames2425', 'L1Games2425', 'BLGames2425','PL2Games2425']  # Reemplaza con los nombres de tus hojas
final_df = read_google_sheets_into_dataframe(sheet_names)
data = final_df[['Home team id'
,'Away team id', '+0 HG', '+1 HG', '+2 HG', '+0 AG', '+1 AG', '+2 AG', 'HG','AG']]
data

import pandas as pd
import numpy as np
from sklearn import preprocessing
import matplotlib.pyplot as plt
plt.rc("font", size=14)
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
import seaborn as sns
sns.set(style="white")
sns.set(style="whitegrid", color_codes=True)

data = data.drop(data[data.eq('nan').any(axis=1)].index)

data['Home team id'] = pd.to_numeric(data['Home team id'])
data['Away team id'] = pd.to_numeric(data['Away team id'])
data['HG'] = pd.to_numeric(data['HG'])
data['AG'] = pd.to_numeric(data['AG'])
data['+0 HG'] = pd.to_numeric(data['+0 HG'])
data['+1 HG'] = pd.to_numeric(data['+1 HG'])
data['+2 HG'] = pd.to_numeric(data['+2 HG'])
data['+0 AG'] = pd.to_numeric(data['+0 AG'])
data['+1 AG'] = pd.to_numeric(data['+1 AG'])
data['+2 AG'] = pd.to_numeric(data['+2 AG'])

def asignar_valor(row):
    suma_goles = (row['HG']) + (row['AG'])
    return suma_goles


# Aplicar la función a cada fila del DataFrame
data['y'] = data.apply(lambda row: asignar_valor(row), axis=1)

X = data.drop(['y'], axis  = 1, inplace = False)

y = pd.DataFrame(data['y'])
#X = X[['Home team id','Away team id', '+0 HG', '+1 HG', '+2 HG', '+0 AG', '+1 AG', '+2 AG']]

X = X[[ '+0 HG', '+1 HG', '+2 HG', '+0 AG', '+1 AG', '+2 AG']]

import pandas as pd
from sklearn.tree import DecisionTreeRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score
# Assuming X and y are your feature matrix and target vector
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# Create Decision Tree regressor object
clf = DecisionTreeRegressor()

# Train Decision Tree regressor
clf.fit(X_train, y_train)

# Predict the response for test dataset
y_pred = clf.predict(X_test)

# Calculate mean absolute error
mae = mean_absolute_error(y_test, y_pred)



print("Mean Absolute Error:", mae)
mse = mean_squared_error(y_test, y_pred)

def prepare(df):
#df = df[['Home team id', 'Away team id','+0 HG','+1 HG','+2 HG','+0 AG','+1 AG','+2 AG']]
 df = df[['+0 HG','+1 HG','+2 HG','+0 AG','+1 AG','+2 AG']]
 return df



def asignar_valor(row):
    suma_goles = (row['HG']) + (row['AG'])
    return suma_goles


def alg2(sheetname,df):

  final_df = read_google_sheets_into_dataframe(sheetname)
  data = final_df[['Home team id'
  ,'Away team id', '+0 HG', '+1 HG', '+2 HG', '+0 AG', '+1 AG', '+2 AG', 'HG','AG']]
  data = data.drop(data[data.eq('nan').any(axis=1)].index)

  data['Home team id'] = pd.to_numeric(data['Home team id'])
  data['Away team id'] = pd.to_numeric(data['Away team id'])
  data['HG'] = pd.to_numeric(data['HG'])
  data['AG'] = pd.to_numeric(data['AG'])
  data['+0 HG'] = pd.to_numeric(data['+0 HG'])
  data['+1 HG'] = pd.to_numeric(data['+1 HG'])
  data['+2 HG'] = pd.to_numeric(data['+2 HG'])
  data['+0 AG'] = pd.to_numeric(data['+0 AG'])
  data['+1 AG'] = pd.to_numeric(data['+1 AG'])
  data['+2 AG'] = pd.to_numeric(data['+2 AG'])




# Aplicar la función a cada fila del DataFrame
  data['y'] = data.apply(lambda row: asignar_valor(row), axis=1)

  X = data.drop(['y'], axis  = 1, inplace = False)

  y = pd.DataFrame(data['y'])
  X = X[[ '+0 HG', '+1 HG', '+2 HG', '+0 AG', '+1 AG', '+2 AG']]

  X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

  clf = DecisionTreeRegressor()

  clf.fit(X_train, y_train)



  df = prepare(df)
  y_pred = clf.predict(df)
  return y_pred
def gamesToHist(f,gameWeek,excelName,histName,pageNum):
  df = f(gameWeek)
  dfPred = prepare(df)
  y_pred1 = clf.predict(dfPred)
  y_pred2 = alg2(excelName,dfPred)
  df = df.drop(['Home team id','Away team id','heg','aeg','HG','AG'],axis=1)
  df['A1']=y_pred1
  df['A2']=y_pred2
  df['Result']=''
  df['Bet']=''

  worksheet = gc.open(histName)
  sheet_name = f"Sheet{pageNum}"
  worksheet.add_worksheet(title=sheet_name, rows=1, cols=1)
  sheet = worksheet.worksheet(sheet_name)
  sheet.update([df.columns.values.tolist()] + df.fillna(-1).values.tolist())
def paint_result(spreadsheet_name):
  """Paints the "Result" column light green if the difference between A1 and A2 is less than 2,
     for all sheets in the Google Sheet.

  Args:
    spreadsheet_name: The name of the Google Sheet.
  """

  # Open the Google Sheet.
  spreadsheet = gc.open(spreadsheet_name)

  # Get all worksheets in the spreadsheet.
  worksheets = spreadsheet.worksheets()

  # Iterate over each worksheet.
  for sheet in worksheets:
    try:
      # Get the header row to find column indices.
      header_row = sheet.row_values(1)  # Assuming header is in row 1
      a1_index = header_row.index("A1")
      a2_index = header_row.index("A2")
      result_index = header_row.index("Bet")  # Get "Result" column index

      # Get all values from the sheet.
      values = sheet.get_all_values()

      # Iterate over the rows, skipping the header row.
      for i in range(1, len(values)):
        try:
          # Get the values from columns A1 and A2 using their indices.
          value_a1 = float(values[i][a1_index])
          value_a2 = float(values[i][a2_index])

          # Check if the difference is less than 2.
          if abs(value_a1 - value_a2) < 2:
            # Paint the "Result" column light green.
            sheet.format(f"{chr(ord('A') + result_index)}{i + 1}", {"backgroundColor": {"red": 0.9, "green": 1, "blue": 0.9}})


        except ValueError:
          print(f"Skipping row {i + 1} in sheet '{sheet.title}' due to invalid numeric values in A1 or A2.")
    except ValueError:
      print(f"Skipping sheet '{sheet.title}' due to missing 'A1', 'A2', or 'Result' columns in the header.")
    except gspread.exceptions.APIError as e:
      if "Quota exceeded" in str(e):
        print("Rate limit exceeded. Waiting for 60 seconds...")
        time.sleep(60)  # Wait for 60 seconds before trying again
        # Optionally, re-attempt processing the current sheet
        # ...
      else:
        print(f"Error processing sheet '{sheet.title}': {e}")



#Añadir +1



def process_bet_column_by_name(spreadsheet_name):
    """
    Procesa la columna 'Bet' actualizando su valor a '+1' o '-4' basado en las condiciones de las columnas 'A1' y 'A2',
    para todas las hojas del Google Sheet. Además, copia las filas que cumplen la condición a una nueva hoja 'Summary'.

    Args:
        spreadsheet_name: El nombre del archivo de Google Sheet.
    """
    import gspread
    from google.auth import default

    # Autenticación para acceder a Google Sheets y Google Drive
    creds, _ = default()
    gc = gspread.authorize(creds)

    # Abrir el archivo de Google Sheets
    try:
        spreadsheet = gc.open(spreadsheet_name)
    except gspread.exceptions.SpreadsheetNotFound:
        raise ValueError(f"El archivo '{spreadsheet_name}' no fue encontrado en tu Google Drive.")

    # Crear o acceder a la hoja "Summary"
    try:
        summary_sheet = spreadsheet.worksheet("Summary")
    except gspread.exceptions.WorksheetNotFound:
        summary_sheet = spreadsheet.add_worksheet(title="Summary", rows="1000", cols="26")
        summary_sheet.append_row( ["Column " + chr(65 + i) for i in range(26)])  # Encabezado

    # Obtener todas las hojas en el archivo
    worksheets = spreadsheet.worksheets()
    row_counter = 0
    rc = 0

    # Limpiar la hoja "Summary" excepto el encabezado
    summary_sheet.clear()
    summary_sheet.append_row( ["Column " + chr(65 + i) for i in range(26)])  # Nuevo encabezado

    # Iterar sobre todas las hojas (worksheets)
    for sheet in worksheets:
        if sheet.title == "Summary":
            continue  # Saltar la hoja "Summary"

        try:
            # Obtener la fila de encabezado para encontrar los índices de las columnas
            header_row = sheet.row_values(1)  # Asumimos que los encabezados están en la primera fila
            a1_index = header_row.index("A1")
            a2_index = header_row.index("A2")
            bet_index = header_row.index("Bet")  # Obtener el índice de la columna "Bet"

            # Obtener todos los valores de la hoja
            values = sheet.get_all_values()

            # Iterar sobre las filas, omitiendo la fila de encabezado
            for i in range(1, len(values)):
                try:
                    # Obtener los valores de las columnas A1 y A2 usando sus índices
                    value_a1 = float(values[i][a1_index])
                    value_a2 = float(values[i][a2_index])

                      # Comprobar si la diferencia es menor a 2 y la suma mayor o igual a 5
                    if abs(value_a1 - value_a2) < 2 and (value_a1 + value_a2) >= 5:
                        sheet.update_cell(i + 1, bet_index + 1, "'+1")
                        summary_sheet.append_row(values[i])  # Copiar fila a "Summary"
                        row_counter += 1
                        rc += 1

                    elif abs(value_a1 - value_a2) < 2 and (value_a1 + value_a2) < 5:
                        if value_a1== 2 and value_a2== 2:
                            continue
                        else:
                            sheet.update_cell(i + 1, bet_index + 1, "'-4")
                            summary_sheet.append_row(values[i])  # Copiar fila a "Summary"
                            row_counter += 1
                            rc += 1

                    # Insertar una fila en blanco cada 3 filas copiadas
                    if row_counter == 3:
                        summary_sheet.append_row(["------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"] )  # Insertar fila vacía
                        row_counter = 0

                except ValueError:
                    print(f"Saltando fila {i + 1} en la hoja '{sheet.title}' debido a valores no válidos en A1 o A2.")
        except ValueError:
            print(f"Saltando hoja '{sheet.title}': No tiene las columnas requeridas 'A1', 'A2' o 'Bet'.")
        except gspread.exceptions.APIError as e:
            if "Quota exceeded" in str(e):
                print("Límite de solicitudes excedido. Esperando 60 segundos...")
                time.sleep(60)
            else:
                print(f"Error procesando la hoja '{sheet.title}': {e}")

    print(f"Todas las hojas han sido procesadas. La hoja 'Summary' ha sido actualizada. Hay un total de '{rc}' partidos")



#gamesToHist(matchDayBLStats,34,['BLGames2425'],'HIST322425',1)
#gamesToHist(matchDayL1Stats,34,['L1Games2425'],'HIST322425',2)
#gamesToHist(matchDayPDStats,36,['PDGames2425'],'HIST322425',3)
#gamesToHist(matchDaySAStats,37,['SAGames2425'],'HIST322425',5)
#gamesToHist(matchDayPLStats,37,['PLGames2425'],'HIST322425',4)
#gamesToHist(matchDayPL2Stats,43,['PL2Games2425'],'HIST312425',6)
#process_bet_column_by_name("HIST322425")
#paint_result('HIST322425')





