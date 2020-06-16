# Constants
BOOKMAKER_SPORTS_TABLE = 'bookmaker_sports'
BOOKMAKER_TOURNAMENTS_TABLE = 'bookmaker_tournaments'
BOOKMAKER_TEAMS_TABLE = 'bookmaker_teams'
BOOKMAKER_MARKETS_TABLE = 'bookmaker_markets'
EVENTS_TABLE = 'events'
EVENT_TEAMS_TABLE = 'event_teams'
BOOKMAKER_EVENTS_TABLE = 'bookmaker_events'
BOOKMAKER_EVENT_MARKETS_TABLE = 'bookmaker_event_markets'
BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE = 'bookmaker_event_market_outcomes'

# Variables
bookmaker_sports = {}
bookmaker_tournaments = {}
bookmaker_teams = {}
bookmaker_markets = {}

sports_maps = {}
tournaments_maps = {}
teams_maps = {}
markets_maps = {}

bookmaker_sports_to_skip = []
bookmaker_tournaments_to_skip = []

processed_files = 0
events_processed = 0
processedEntities = {
    BOOKMAKER_SPORTS_TABLE: 0,
    BOOKMAKER_TOURNAMENTS_TABLE: 0,
    BOOKMAKER_TEAMS_TABLE: 0,
    BOOKMAKER_MARKETS_TABLE: 0,
    EVENTS_TABLE: 0,
    EVENT_TEAMS_TABLE: 0,
    BOOKMAKER_EVENTS_TABLE: 0,
    BOOKMAKER_EVENT_MARKETS_TABLE: 0,
    BOOKMAKER_EVENT_MARKET_OUTCOMES_TABLE: 0
}

processed_bookmaker_sports = []
processed_bookmaker_tournaments = []
processed_bookmaker_teams = []
processed_bookmaker_markets = []
processed_bookmaker_bookmaker_markets = []
processed_bookmaker_event_markets = [];
processed_events = []
processed_event_teams = []

events = []

def shouldSkipSport(sport):
	return sport in bookmaker_sports_to_skip

def shouldSkipTournament(tournament):
	return tournament in bookmaker_tournaments_to_skip

def processEvent(bookmaker_event):
	global events_processed

	if (len(bookmaker_event.title) > 0 
		and len(bookmaker_event.sport) > 0 
		and len(bookmaker_event.tournament) > 0 
		and (len(bookmaker_event.date) > 0 or bookmaker_event.live)
		and not shouldSkipSport(bookmaker_event.sport)
		and not shouldSkipTournament(bookmaker_event.tournament)):

		buildBookmakerSport(bookmaker_event)
		buildBookmakerTournament(bookmaker_event)
		buildBookmakerTeams(bookmaker_event)
		buildBookmakerMarkets(bookmaker_event)
		buildEvent(bookmaker_event)
		events_processed += 1


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

def getLiveDateBySport(live_date_interval = None, date = None):
	a = 1