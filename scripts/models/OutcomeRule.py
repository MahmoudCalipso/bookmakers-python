class OutcomeRule():

	id = 0
	sport_title = ''
	market_title = ''
	input = ''
	input_replace = []
	output = ''

	def __str__(self):
		return "sport_title: {0}, market_title: {1}, input: {2}, output: {3}".format(self.sport_title, self.market_title, self.input, self.output)