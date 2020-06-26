import re

class MarketParser():
    
    CONSTANT = '@scannerbet.constants.{variable}@'

    # Variables
    VARIABLE_BOOKMAKER_TEAM_ANY = '@scannerbet.bookmaker.teams.any@';
    VARIABLE_BOOKMAKER_TEAM_1 = '@scannerbet.bookmaker.teams.team1@';
    VARIABLE_BOOKMAKER_TEAM_2 = '@scannerbet.bookmaker.teams.team2@';
    VARIABLE_TEAM_ANY = '@scannerbet.teams.any@';
    VARIABLE_TEAM_1 = '@scannerbet.teams.team1@';
    VARIABLE_TEAM_2 = '@scannerbet.teams.team2@';
    VARIABLE_DECIMAL = '@scannerbet.global.decimal@';
    VARIABLE_INTEGER = '@scannerbet.global.integer@';
    VARIABLE_OUTCOME = '@scannerbet.global.outcome@';

    REGEX_REPLACE = {
        VARIABLE_BOOKMAKER_TEAM_ANY: '(?![\s.]+$)[a-zA-ZÀ-ÖØ-öø-ÿ\-\,\s.]*',
        VARIABLE_BOOKMAKER_TEAM_1: '(?![\s.]+$)[a-zA-ZÀ-ÖØ-öø-ÿ\-\,\s.]*',
        VARIABLE_BOOKMAKER_TEAM_2: '(?![\s.]+$)[a-zA-ZÀ-ÖØ-öø-ÿ\-\,\s.]*',
        VARIABLE_TEAM_ANY: '(?![\s.]+$)[a-zA-ZÀ-ÖØ-öø-ÿ\-\,\s.]*',
        VARIABLE_TEAM_1: '(?![\s.]+$)[a-zA-ZÀ-ÖØ-öø-ÿ\-\,\s.]*',
        VARIABLE_TEAM_2: '(?![\s.]+$)[a-zA-ZÀ-ÖØ-öø-ÿ\-\,\s.]*',
        VARIABLE_DECIMAL: '[0-9]+[\.|\,]?[0-9]*',
        VARIABLE_INTEGER: '[0-9]+',
    };

    constants = {}

    markets_rules = []

    outcomes_rules = []

    bookmaker_teams = {}

    teams_maps = []

    missing_bookmaker_teams = []

    matching_outcome_rule = None

    def getMissingBookmakerTeams(self):
        return self.missing_bookmaker_teams

    def getMatchingOutcomeRule(self):
        return self.matching_outcome_rule

    def setConstantsVariables(self, constants):
        self.constants = constants

    def setMarketsRules(self, rules):
        final = {}

        for rule in rules:
            if rule.sport_title not in final:
                final[rule.sport_title] = []

            final[rule.sport_title].append(rule)

        self.markets_rules = final

    def setOutcomesRules(self, rules):
        final = {}

        for rule in rules:
            if rule.sport_title not in final:
                final[rule.sport_title] = {}

            if rule.market_title not in final[rule.sport_title]:
                final[rule.sport_title][rule.market_title] = []

            final[rule.sport_title][rule.market_title].append(rule)

        self.outcomes_rules = final

    def setBookmakerTeams(self, bookmaker_teams):
        self.bookmaker_teams = bookmaker_teams

    def setTeamsMap(self, teams_map):
        self.teams_maps = teams_map

    def getMarketRule(self, bookmaker_market_title, sport_title):
        output = None

        # Get outcomes rules for this market
        if sport_title in self.markets_rules:
            rules = self.markets_rules[sport_title]
            for rule in rules:
                input_rule = rule.input
                _bookmaker_market_title = bookmaker_market_title

                # Check if something should be replaced in input rule
                if rule.input_replace:
                    for search in rule.input_replace:
                        replace = rule.input_replace[search]
                        input_rule = self.replaceConstants(input_rule)
                        input_rule = input_rule.replace(search, replace)
                        _bookmaker_market_title = bookmaker_market_title.replace(search, replace)

                input = self.filterInput(input_rule, _bookmaker_market_title)

                # Check that given input actually matches the outcome title
                if input != _bookmaker_market_title:
                    continue

                output = rule
                break

        return output

    def filterMarketOutcomes(self, market_rule, bookmaker_market_title, outcomes, teams):
        output = []

        for outcome in outcomes:
            try:
                outcome_output = self.replaceConstants(market_rule.outcome_output)

                if outcome_output.find(VARIABLE_OUTCOME) != -1:
                    outcome_output = outcome_output.replace(VARIABLE_OUTCOME, outcome.title)

                # Replace variables
                for variable in self.REGEX_REPLACE:
                    regex = self.REGEX_REPLACE[variable]

                    if outcome_output.find(variable) != -1:
                        matches = re.findall(regex, bookmaker_market_title)

                        if len(matches) > 0:
                            for match in matches:
                                if variable == self.VARIABLE_DECIMAL:
                                    outcome_output = outcome_output.replace(variable, match, 1)
                                    outcome_output = outcome_output.replace(',', '.')
                                elif variable == self.VARIABLE_INTEGER:
                                    outcome_output = outcome_output.replace(variable, match, 1)
                                elif variable == self.VARIABLE_BOOKMAKER_TEAM_ANY:
                                    outcome_output = outcome_output.replace(variable, match.strip())
                                elif variable == self.VARIABLE_BOOKMAKER_TEAM_1:
                                    outcome_output = outcome_output.replace(variable, teams[0].title)
                                elif variable == self.VARIABLE_BOOKMAKER_TEAM_2:
                                    outcome_output = outcome_output.replace(variable, teams[1].title)

                if outcome_output.find('@scannerbet.') == -1:
                    outcome.title = outcome_output
            except:
                pass

            output.append(outcome)

        return output

    def getMarketOutcome(self, outcome_title, sport_title, market_title, teams):
        output = ''

        # Clear missing bookmaker teams
        self.missing_bookmaker_teams = []
        self.matching_outcome_rule = None

        # Get outcomes rules for this market
        if sport_title in self.outcomes_rules and market_title in self.outcomes_rules[sport_title]:
            rules = self.outcomes_rules[sport_title][market_title]

            for rule in rules:
                input_rule = rule.input
                output_rule = rule.output
                _outcome_title = outcome_title

                # Check if something should be replaced in input rule
                if rule.input_replace:
                    for search in rule.input_replace:
                        replace = rule.input_replace[search]
                        input_rule = self.replaceConstants(input_rule)
                        input_rule = input_rule.replace(search, replace)
                        _outcome_title = _outcome_title.replace(search, replace)

                input = self.filterInput(input_rule, _outcome_title, teams)

                # Check that given input actually matches the outcome title
                if input != _outcome_title:
                    continue

                output = self.filterOutput(output_rule, _outcome_title, teams)

                # Don't continue if there is already an output or missing bookmaker teams have been found
                if len(output) > 0 or output.isnumeric():
                    self.matching_outcome_rule = rule
                    break

        return output

    def filterInput(self, input_rule, outcome_title, teams = []):
        output = input_rule
        constants_values = []

        for constant in self.constants:
            constants_values.append(self.constants[constant])

        output = self.replaceConstants(output)

        # Replace variables
        for variable in self.REGEX_REPLACE:
            regex = self.REGEX_REPLACE[variable]

            if input_rule.find(variable) != -1:
                matches = re.findall(regex, outcome_title)

                if len(matches) > 0:
                    for match in matches:
                        original_match = match
                        match = match.lstrip()
                        _match = match.strip()

                        if (
                            (len(_match) > 0 or _match.isnumeric())
                            and input_rule.find(variable) != -1
                            and (_match not in constants_values or variable == self.VARIABLE_DECIMAL or variable == self.VARIABLE_INTEGER)
                        ):
                            if variable == self.VARIABLE_BOOKMAKER_TEAM_ANY or variable == self.VARIABLE_BOOKMAKER_TEAM_1 or variable == self.VARIABLE_BOOKMAKER_TEAM_2:
                                if _match.endswith('-') and (output.endswith(self.VARIABLE_INTEGER) or output.endswith(self.VARIABLE_DECIMAL)):
                                    _match = _match.rstrip('-')
                                    _match = _match.strip()
                                elif output != self.VARIABLE_TEAM_1 and output != self.VARIABLE_TEAM_2 and output != self.VARIABLE_TEAM_ANY:
                                    # Get numbers from outcome title and check if the combination with $match matches a bookmaker team
                                    number_matches = re.findall(self.REGEX_REPLACE[self.VARIABLE_INTEGER], outcome_title)

                                    if len(number_matches) > 0 and _match not in self.bookmaker_teams:
                                        for number_match in number_matches:
                                            _number_match = match + number_match
                                            _number_match_reverse = number_match + original_match

                                            if _number_match in self.bookmaker_teams and outcome_title.find(_number_match) != -1:
                                                _match = _number_match
                                            elif _number_match_reverse in self.bookmaker_teams and outcome_title.find(_number_match_reverse) != -1:
                                                _match = _number_match_reverse
                                            else:
                                                # Loop through characters and check if any combination coincides with an existing bookmaker team
                                                i = 0

                                                while i < len(original_match):
                                                    start = original_match[0:i + 1]
                                                    end_index = len(original_match) - 1 if i + 1 >= len(original_match) - 1 else i + 1
                                                    end = original_match[end_index]
                                                    title = start + number_match + end

                                                    if title in self.bookmaker_teams and outcome_title.find(title) != -1:
                                                        _match = title
                                                        break
                                                    
                                                    i += 1

                                if len(teams) > 0:
                                    # Check if this bookmaker team is mapped
                                    if _match in self.bookmaker_teams:
                                        if self.bookmaker_teams[_match]['id'] in self.teams_maps:
                                            team = self.teams_maps[self.bookmaker_teams[_match]['id']]
                                            for bookmaker_event_team in teams:
                                                if (
                                                    bookmaker_event_team.title in self.bookmaker_teams
                                                    and self.bookmaker_teams[bookmaker_event_team.title]['id'] in self.teams_maps
                                                    and self.teams_maps[self.bookmaker_teams[bookmaker_event_team.title]['id']]['team_title'] == team['team_title']
                                                    and (
                                                        variable == self.VARIABLE_BOOKMAKER_TEAM_ANY
                                                        or (variable == self.VARIABLE_BOOKMAKER_TEAM_1 and bookmaker_event_team.local)
                                                        or (variable == self.VARIABLE_BOOKMAKER_TEAM_2 and not bookmaker_event_team.local)
                                                    )
                                                ):
                                                    output = output.replace(variable, _match)
                                                    break

                                    elif outcome_title not in self.missing_bookmaker_teams:
                                        self.missing_bookmaker_teams.append(outcome_title)
                                elif len(_match) > 0:
                                    output = output.replace(variable, _match)

                            elif variable == self.VARIABLE_INTEGER or variable == self.VARIABLE_DECIMAL:
                                output = output.replace(variable, _match, 1)
                            else:
                                output = output.replace(variable, _match)


        return output

    def filterOutput(self, output_rule, outcome_title, teams):
        output = ''
        constants_values = []

        for constant in self.constants:
            constants_values.append(self.constants[constant])

        output_rule = self.replaceConstants(output_rule)

        # Replace variables
        for variable in self.REGEX_REPLACE:
            regex = self.REGEX_REPLACE[variable]

            if output_rule.find(variable) != -1:
                matches = re.findall(regex, outcome_title)

                if len(matches) > 0:
                    no_team_matches = 0
                    for match in matches:
                        original_match = match
                        _match = match.strip()

                        if (
                            (len(_match) > 0 or _match.isnumeric())
                            and (_match not in constants_values or variable == self.VARIABLE_DECIMAL or variable == self.VARIABLE_INTEGER)
                        ):
                            if variable == self.VARIABLE_DECIMAL:
                                output_rule = re.sub(variable + '(?!\{key)', variable + '{key=' + _match + '}', output_rule, 1)
                                output_rule = output_rule.replace(',', '.')
                            elif variable == self.VARIABLE_INTEGER:
                                output_rule = re.sub(variable + '(?!\{key)', variable + '{key=' + _match + '}', output_rule, 1)
                            elif variable == self.VARIABLE_TEAM_ANY or variable == self.VARIABLE_TEAM_1 or variable == self.VARIABLE_TEAM_2:
                                if _match.endswith('-') and (output_rule.endswith(self.VARIABLE_INTEGER) or output_rule.endswith(self.VARIABLE_DECIMAL)):
                                    _match = _match.rstrip('-')
                                    _match = _match.strip()
                                elif output_rule != self.VARIABLE_TEAM_1 and output_rule != self.VARIABLE_TEAM_2 and output_rule != self.VARIABLE_TEAM_ANY:
                                    # Get numbers from outcome title and check if the combination with $match matches a bookmaker team
                                    number_matches = re.findall(self.REGEX_REPLACE[self.VARIABLE_INTEGER], outcome_title)

                                    if len(number_matches) > 0 and _match not in self.bookmaker_teams:
                                        for number_match in number_matches:
                                            _number_match = match + number_match
                                            _number_match_reverse = number_match + original_match

                                            if _number_match in self.bookmaker_teams and outcome_title.find(_number_match) != -1:
                                                _match = _number_match
                                            elif _number_match_reverse in self.bookmaker_teams and outcome_title.find(_number_match_reverse) != -1:
                                                _match = _number_match_reverse
                                            else:
                                                # Loop through characters and check if any combination coincides with an existing bookmaker team
                                                i = 0

                                                while i < len(original_match):
                                                    start = original_match[0:i + 1]
                                                    end_index = len(original_match) - 1 if i + 1 >= len(original_match) - 1 else i + 1
                                                    end = original_match[end_index]
                                                    title = start + number_match + end

                                                    if title in self.bookmaker_teams and outcome_title.find(title) != -1:
                                                        _match = title
                                                        break
                                                    
                                                    i += 1

                                # Check if this bookmaker team is mapped
                                if _match in self.bookmaker_teams:
                                    if self.bookmaker_teams[_match]['id'] in self.teams_maps:
                                        team = self.teams_maps[self.bookmaker_teams[_match]['id']]

                                        if variable == self.VARIABLE_TEAM_ANY:
                                            output_rule = output_rule.replace(self.VARIABLE_TEAM_ANY, self.VARIABLE_TEAM_ANY + '{key=' + str(team['team_id']) + '}')
                                elif outcome_title not in self.missing_bookmaker_teams:
                                    self.missing_bookmaker_teams.append(outcome_title)
                        else:
                            no_team_matches += 1

                    if (
                        len(matches) == no_team_matches
                        and (variable == self.VARIABLE_TEAM_1 or variable == self.VARIABLE_TEAM_2)
                        and len(teams) == 2
                    ):
                        team_title = teams[0].title if variable == self.VARIABLE_TEAM_1 else teams[1].title

                        # Check if this bookmaker team is mapped
                        if (
                            (team_title not in self.bookmaker_teams or self.bookmaker_teams[team_title]['id'] not in self.teams_maps)
                            and outcome_title not in self.missing_bookmaker_teams
                        ):
                            self.missing_bookmaker_teams.append(outcome_title)

        if len(output) == 0 and (output_rule == self.VARIABLE_TEAM_1 or output_rule == self.VARIABLE_TEAM_2):
            team_title = teams[0].title if variable == self.VARIABLE_TEAM_1 else teams[1].title
            # Check if this bookmaker team is mapped
            if (
                (team_title not in self.bookmaker_teams or self.bookmaker_teams[team_title]['id'] not in self.teams_maps)
                and outcome_title not in self.missing_bookmaker_teams
            ):
                self.missing_bookmaker_teams.append(outcome_title)
            else:
                _team = self.teams_maps[self.bookmaker_teams[team_title]['id']]
                output = output_rule.replace(output_rule, self.VARIABLE_TEAM_ANY + '{key=' + _team['team_id'] + '}')
        else:
            output = output_rule

        return output

    def replaceConstants(self, subject):
        output = subject

        # Replace constants
        for variable in self.constants:
            replacement = self.constants[variable]
            output = output.replace(variable, replacement)

        return output