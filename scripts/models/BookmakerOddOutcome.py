class BookmakerOddOutcome():

	outcome_id = ''
	title = ''
	decimal = 0
	deep_link = None

	def __init__(self):
		self.outcome_id = ''
		self.title = ''
		self.decimal = 0
		self.deep_link = None

	def __str__(self):
		return "outcome_id: {0}, title: {1}, decimal: {2}, deep_link: {3}".format(self.outcome_id, self.title, self.decimal, self.deep_link)