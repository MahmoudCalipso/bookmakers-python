class BookmakerEvent():
    
    event_id = ''
    title = ''
    tournament = ''
    sport = ''
    tournament = ''
    date = ''
    replace_title = ''
    odds = []
    teams = []
    has_markets = False
    live = False

    def __str__(self):
        return "event_id: {0}, title: {1}, tournament: {2}, date: {3}, odds: {4}, teams: {5}".format(self.event_id, self.title, self.tournament, self.date, self.odds, self.teams)