def shouldSkipSport(sport):
	return False

def shouldSkipTournament(tournament):
	return False

def processEvent(bookmaker_event):
	print('Process event: ')
	print('--- Name => ' + bookmaker_event.title)
	print('--- Sport => ' + bookmaker_event.sport)
	print('--- Tournament => ' + bookmaker_event.tournament)
	print('--- Date => ' + bookmaker_event.date)

	if len(bookmaker_event.title) > 0 
		and len(bookmaker_event.sport) > 0 
		and len(bookmaker_event.tournament) > 0 
		and (len(bookmaker_event.date) > 0 or bookmaker_event.live)
		and not shouldSkipSport(bookmaker_event.sport)
		and not shouldSkipTournament(bookmaker_event.tournament):


def buildBookmakerSport(bookmaker_event):
	a = 1

def buildBookmakerTournament(bookmaker_event):
	a = 1

def buildBookmakerTeams(bookmaker_event):
	a = 1

def buildBookmakerMarkets(bookmaker_event):
	a = 1

def buildEvent(bookmaker_event):
	a = 1

def buildEventTeams(bookmaker_event, event_title, event_date):
	a = 1

def buildEventTeam(bookmaker_event, team, event_title, event_date, index):
	a = 1

def buildBookmakerEvent(bookmaker_event, event_title, event_date):
	a = 1

def buildBookmakerEventMarkets(bookmaker_event, event_title, event_date):
	a = 1

def getLiveDateBySport(live_date_interval = None, date = None)