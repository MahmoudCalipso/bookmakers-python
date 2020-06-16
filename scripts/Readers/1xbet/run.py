import ijson
import requests
import time
import os
import csv
import sys
import re
from datetime import datetime, timedelta
sys.path.append("../")
sys.path.append("../../")
from models import BookmakerEvent, BookmakerEventTeam, BookmakerEventTeamMember, BookmakerOdd, BookmakerOddOutcome
import bookmaker_updater

# Constants
MARKET_1X2 = '1X2';
MARKET_DOUBLE_CHANCE = 'Double chance';
MARKET_HANDICAP = 'Handicap';
MARKET_TOTAL = 'Total';
MARKET_INDIVIDUAL_TOTAL_1 = 'Individual Total 1';
MARKET_INDIVIDUAL_TOTAL_2 = 'Individual Total 2';
MARKET_HF_FT = 'HT-FT';
MARKET_WIN_IN_THE_GAME = 'Win In The Game';
MARKET_WIN_IN_THE_SET = 'Win In The Set';
MARKET_HALF_1X2 = 'Half :1x2';
MARKET_HALF_OVER_UNDER = 'Half :Over/Under';
MARKET_PENALTY_SHOTS = 'Penalty Shots Including Overtime';
MARKET_DRAW = 'Draw';
MARKET_BOTH_TEAMS_TO_SCORE = 'Both Teams To Score';
MARKET_EVEN_ODD = 'Even/Odd';
MARKET_SCORED_GOAL = 'Scored Goal';
MARKET_HALVES_SCORING = 'Halves Scoring';
MARKET_WIN_IN_THE_INNING = 'Win In The Inning';
MARKET_NEXT_GOAL = 'Next Goal';
MARKET_WINNER = 'Winner';
MARKET_3W_HANDICAP = '3W Handicap';
MARKET_BOTH_HALVES_GOAL = 'Both Halves Goal';
MARKET_FIRST_MATCH_GOAL = 'First Match Goal';
MARKET_HALVES_TOTAL_OF_GOALS = 'Halves Total of Goals';
MARKET_DRAW_IN_EITHER_HALF = 'Draw In Either Half';
MARKET_1ST_HALF_OR_MATCH_1X2 = '1st Half or Match: 1X2';
MARKET_SCORE_IN_GAME = 'Score in Game';
MARKET_TIE_BREAK = 'Tie Break';
MARKET_GOAL_IN_HALF = 'Goal In Half';
MARKET_CORRECT_SCORE = 'Correct Score';
MARKET_SETS_HANDICAP = 'Sets Handicap';
MARKET_INDIVIDUAL_TOTAL_1_ODD_EVEN = 'Individual Total 1 Odd/Even';
MARKET_INDIVIDUAL_TOTAL_2_ODD_EVEN = 'Individual Total 2 Odd/Even';
MARKET_TOTAL_SETS = 'Total Sets';
MARKET_PERIODS_SCORING = 'Periods Scoring';
MARKET_TO_WIN_IN_SHOOTOUTS = 'To Win In Shootouts';
MARKET_END_RESULT = 'End Result';
MARKET_WHO_WILL_WIN_YES_NO = 'Who will win - Yes/No';
MARKET_TEAM_TO_SCORE_IN_EACH_PERIOD_YES_NO = 'Team To Score In Each Period - Yes/No';
MARKET_END_WINNER = 'End Winner';
MARKET_TOTAL_SETS_OVER_UNDER = 'Total Sets Over/Under';
MARKET_OVERTIME_WIN = 'Overtime Win';
MARKET_PENALTY_SHOTS_YES_NO = 'Penalty Shots';
MARKET_BREAK = 'Break';
MARKET_TOTAL_EACH_TEAM_WILL_SCORE_UNDER_OVER = 'Total Each Team Will Score Under/Over';
MARKET_TOTAL_SCORING_PERIOD = 'Total Scoring Period';
MARKET_BOTH_TO_SCORE = 'Both To Score';
MARKET_HF_FT_TOTAL = 'HF-FT + Total';
MARKET_TOTAL_FRAMES = 'Total Frames';
MARKET_PLAYER_WINS_SET = 'Player Wins Set';
MARKET_WHO_WILL_WIN_SETS = 'Who will win sets';
MARKET_TOTAL_OVER_END = 'Total over end';
MARKET_SETS_SCORE = 'Sets score';
MARKET_NUMBET_OF_GOALS = 'Number of Goals';
MARKET_TOTAL_SHOTS_IN_MATCH = 'Total Shots In The Match';
MARKET_GOALS_SCORED_IN_A_MATCH = 'Goals Scored In A Match';
MARKET_DRIVER_TO_WIN = 'Driver To Win';
MARKET_KNOCKDOWN = 'Knockdown';
MARKET_TEAM_TOTAL_GOALS = 'Team Total Goals';
MARKET_TOTAL_TEAM = 'Total Team';
MARKET_FIGHT_TO_GO_THE_DISTANCE = 'Fight To Go The Distance';
MARKET_TOTAL_ROUNDS = 'Total Rounds';
MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED = 'When The Winner Will Be Determined';
MARKET_ASIAN_TOTAL = 'Asian Total';
MARKET_ASIAN_HANDICAP = 'Asian Handicap';

outcomes = {
    '1': {
        'title': 'W1',
        'market': MARKET_1X2
    },
    '2': {
        'title': 'X',
        'market': MARKET_1X2
    },
    '3': {
        'title': 'W2',
        'market': MARKET_1X2
    },
    '4': {
        'title': '1X',
        'market': MARKET_DOUBLE_CHANCE
    },
    '5': {
        'title': '12',
        'market': MARKET_DOUBLE_CHANCE
    },
    '6': {
        'title': '2X',
        'market': MARKET_DOUBLE_CHANCE
    },
    '7': {
        'title': 'Handiсap 1 ({replace1})',
        'market': MARKET_HANDICAP
    },
    '8': {
        'title': 'Handiсap 2 ({replace1})',
        'market': MARKET_HANDICAP
    },
    '9': {
        'title': 'Total Over ({replace1})',
        'market': MARKET_TOTAL
    },
    '10': {
        'title': 'Total Under ({replace1})',
        'market': MARKET_TOTAL
    },
    '11': {
        'title': 'Individual Total 1 Over ({replace1})',
        'market': MARKET_INDIVIDUAL_TOTAL_1
    },
    '12': {
        'title': 'Individual Total 1 Under ({replace1})',
        'market': MARKET_INDIVIDUAL_TOTAL_1
    },
    '13': {
        'title': 'Individual Total 2 Over ({replace1})',
        'market': MARKET_INDIVIDUAL_TOTAL_2
    },
    '14': {
        'title': 'Individual Total 2 Under ({replace1})',
        'market': MARKET_INDIVIDUAL_TOTAL_2
    },
    '15': {
        'title': 'HT-FT W1W2',
        'market': MARKET_HF_FT
    },
    '16': {
        'title': 'HT-FT W1W2',
        'market': MARKET_HF_FT
    },
    '17': {
        'title': 'HT-FT W1W2',
        'market': MARKET_HF_FT
    },
    '18': {
        'title': 'HT-FT XW1',
        'market': MARKET_HF_FT
    },
    '19': {
        'title': 'HT-FT XX',
        'market': MARKET_HF_FT
    },
    '20': {
        'title': 'HT-FT XW2',
        'market': MARKET_HF_FT
    },
    '21': {
        'title': 'HT-FT W2W1',
        'market': MARKET_HF_FT
    },
    '22': {
        'title': 'HT-FT W2X',
        'market': MARKET_HF_FT
    },
    '23': {
        'title': 'HT-FT W2W2',
        'market': MARKET_HF_FT
    },
    '50': {
        'title': 'Player 1 To Win Game',
        'market': MARKET_WIN_IN_THE_GAME
    },
    '51': {
        'title': 'Player 2 To Win Game',
        'market': MARKET_WIN_IN_THE_GAME
    },
    '58': {
        'title': 'Player 1 To Win Set',
        'market': MARKET_WIN_IN_THE_SET
    },
    '59': {
        'title': 'Player 2 To Win Set',
        'market': MARKET_WIN_IN_THE_SET
    },
    '77': {
        'title': 'Drawn First Half - Yes',
        'market': MARKET_HALF_1X2
    },
    '78': {
        'title': 'Drawn First Half - Yes',
        'market': MARKET_HALF_1X2
    },
    '79': {
        'title': 'Drawn First Half - Yes',
        'market': MARKET_HALF_1X2
    },
    '80': {
        'title': 'Team 1 To Win First Half - No',
        'market': MARKET_HALF_1X2
    },
    '81': {
        'title': 'Team 2 To Win First Half - Yes',
        'market': MARKET_HALF_1X2
    },
    '82': {
        'title': 'Team 2 To Win First Half - Yes',
        'market': MARKET_HALF_1X2
    },
    '83': {
        'title': 'Total 1st Half Over ({replace1})',
        'market': MARKET_HALF_OVER_UNDER
    },
    '84': {
        'title': 'Total 1st Half Under ({replace1})',
        'market': MARKET_HALF_OVER_UNDER
    },
    '123': {
        'title': 'Team 1 To Win Including Overtime And Penalty Shootouts',
        'market': MARKET_PENALTY_SHOTS
    },
    '124': {
        'title': 'Team 2 To Win Including Overtime And Penalty Shootouts',
        'market': MARKET_PENALTY_SHOTS
    },
    '178': {
        'title': 'Draw 0:0 - Yes',
        'market': MARKET_DRAW
    },
    '179': {
        'title': 'Score Draw - Yes',
        'market': MARKET_DRAW
    },
    '180': {
        'title': 'Both Teams To Score - Yes',
        'market': MARKET_BOTH_TEAMS_TO_SCORE
    },
    '181': {
        'title': 'Both Teams To Score - No',
        'market': MARKET_BOTH_TEAMS_TO_SCORE
    },
    '182': {
        'title': 'Total Even - Yes',
        'market': MARKET_EVEN_ODD
    },
    '183': {
        'title': 'Total Even - No',
        'market': MARKET_EVEN_ODD
    },
    '184': {
        'title': 'Team 1 To Score - Yes',
        'market': MARKET_SCORED_GOAL
    },
    '185': {
        'title': 'Team 1 To Score - No',
        'market': MARKET_SCORED_GOAL
    },
    '186': {
        'title': 'Team 2 To Score - Yes',
        'market': MARKET_SCORED_GOAL
    },
    '187': {
        'title': 'Team 2 To Score - No',
        'market': MARKET_SCORED_GOAL
    },
    '188': {
        'title': '1st Half > 2nd Half',
        'market': MARKET_HALVES_SCORING
    },
    '189': {
        'title': '1st Half = 2nd Half',
        'market': MARKET_HALVES_SCORING
    },
    '190': {
        'title': '1st Half < 2nd Half',
        'market': MARKET_HALVES_SCORING
    },
    '215': {
        'title': 'Draw 0:0 - No',
        'market': MARKET_DRAW
    },
    '216': {
        'title': 'Score Draw - No',
        'market': MARKET_DRAW
    },
    '374': {
        'title': 'Team 1 To Win Inning',
        'market': MARKET_WIN_IN_THE_INNING
    },
    '375': {
        'title': 'Team 2 To Win Inning',
        'market': MARKET_WIN_IN_THE_INNING
    },
    '376': {
        'title': 'Inning Draw',
        'market': MARKET_WIN_IN_THE_INNING
    },
    '388': {
        'title': 'Team 1 To Score Next Goal',
        'market': MARKET_NEXT_GOAL
    },
    '389': {
        'title': 'Team 2 To Score Next Goal',
        'market': MARKET_NEXT_GOAL
    },
    '390': {
        'title': 'Neither Team To Score Next Goal',
        'market': MARKET_NEXT_GOAL
    },
    '396': {
        'title': 'Wins {} - Yes',
        'market': MARKET_WINNER
    },
    '397': {
        'title': 'Wins {} - No',
        'market': MARKET_WINNER
    },
    '424': {
        'title': '3 Way ({replace1}) W1',
        'market': MARKET_3W_HANDICAP
    },
    '425': {
        'title': '3 Way ({replace1}) X',
        'market': MARKET_3W_HANDICAP
    },
    '426': {
        'title': '3 Way ({replace1}) W2',
        'market': MARKET_3W_HANDICAP
    },
    '478': {
        'title': 'Goals Scored In Both Halves - Yes',
        'market': MARKET_BOTH_HALVES_GOAL
    },
    '479': {
        'title': 'Goals Scored In Both Halves - No',
        'market': MARKET_BOTH_HALVES_GOAL
    },
    '480': {
        'title': 'First Match Goal - In ({replace1}) To ({replace2}) Minute',
        'market': MARKET_FIRST_MATCH_GOAL
    },
    '481': {
        'title': 'First Match Goal - In ({replace1}) To ({replace2}) Minute',
        'market': MARKET_FIRST_MATCH_GOAL
    },
    '482': {
        'title': 'No First Goal',
        'market': MARKET_FIRST_MATCH_GOAL
    },
    '512': {
        'title': 'Both Halves Over ({replace1}) Goals - Yes',
        'market': MARKET_HALVES_TOTAL_OF_GOALS
    },
    '513': {
        'title': 'Both Halves Over ({replace1}) Goals - No',
        'market': MARKET_HALVES_TOTAL_OF_GOALS
    },
    '514': {
        'title': 'Both Halves Under ({replace1}) Goals - Yes',
        'market': MARKET_HALVES_TOTAL_OF_GOALS
    },
    '515': {
        'title': 'Both Halves Under ({replace1}) Goals - No',
        'market': MARKET_HALVES_TOTAL_OF_GOALS
    },
    '516': {
        'title': 'Draw In At Least One Half - Yes',
        'market': MARKET_DRAW_IN_EITHER_HALF
    },
    '517': {
        'title': 'Draw In At Least One Half - Yes',
        'market': MARKET_DRAW_IN_EITHER_HALF
    },
    '535': {
        'title': '1st Half or Match: 1',
        'market': MARKET_1ST_HALF_OR_MATCH_1X2
    },
    '536': {
        'title': '1st Half or Match: X',
        'market': MARKET_1ST_HALF_OR_MATCH_1X2
    },
    '537': {
        'title': '1st Half or Match: 2',
        'market': MARKET_1ST_HALF_OR_MATCH_1X2
    },
    '538': {
        'title': 'Game ({replace1}): 40:40 Yes',
        'market': MARKET_SCORE_IN_GAME
    },
    '539': {
        'title': 'Game ({replace1}): 40:40 No',
        'market': MARKET_SCORE_IN_GAME
    },
    '540': {
        'title': 'Tie Break - Yes',
        'market': MARKET_TIE_BREAK
    },
    '541': {
        'title': 'Tie Break - No',
        'market': MARKET_TIE_BREAK
    },
    '651': {
        'title': 'First Goal in 1st Half',
        'market': MARKET_GOAL_IN_HALF
    },
    '652': {
        'title': 'First Goal in 2nd Half',
        'market': MARKET_GOAL_IN_HALF
    },
    '653': {
        'title': 'No First Goal',
        'market': MARKET_GOAL_IN_HALF
    },
    '731': {
        'title': 'Correct Score ({replace1})-({replace2})',
        'market': MARKET_CORRECT_SCORE
    },
    '732': {
        'title': 'Player 1 Handiсap ({replace1}) Sets',
        'market': MARKET_SETS_HANDICAP
    },
    '733': {
        'title': 'Player 2 Handiсap ({replace1}) Sets',
        'market': MARKET_SETS_HANDICAP
    },
    '755': {
        'title': 'Team 1 Total Even',
        'market': MARKET_INDIVIDUAL_TOTAL_1_ODD_EVEN
    },
    '757': {
        'title': 'Team 1 Total Odd',
        'market': MARKET_INDIVIDUAL_TOTAL_1_ODD_EVEN
    },
    '766': {
        'title': 'Team 2 Total Even',
        'market': MARKET_INDIVIDUAL_TOTAL_2_ODD_EVEN
    },
    '767': {
        'title': 'Team 2 Total Odd',
        'market': MARKET_INDIVIDUAL_TOTAL_2_ODD_EVEN
    },
    '768': {
        'title': 'Total Sets ({replace1})',
        'market': MARKET_TOTAL_SETS
    },
    '773': {
        'title': 'Will There Be ({replace1}) Period? - Yes',
        'market': MARKET_PERIODS_SCORING
    },
    '774': {
        'title': 'Will There Be ({replace1}) Period? - No',
        'market': MARKET_PERIODS_SCORING
    },
    '775': {
        'title': 'Exact Period Count: ({replace1})',
        'market': MARKET_PERIODS_SCORING
    },
    '776': {
        'title': 'Team 1 To Win In Shootouts - Yes',
        'market': MARKET_TO_WIN_IN_SHOOTOUTS
    },
    '777': {
        'title': 'Team 1 To Win In Shootouts - No',
        'market': MARKET_TO_WIN_IN_SHOOTOUTS
    },
    '778': {
        'title': 'Team 2 To Win In Shootouts - Yes',
        'market': MARKET_TO_WIN_IN_SHOOTOUTS
    },
    '779': {
        'title': 'Team 2 To Win In Shootouts - No',
        'market': MARKET_TO_WIN_IN_SHOOTOUTS
    },
    '780': {
        'title': 'Period ({replace1}) > Period ({replace2}) - Yes',
        'market': MARKET_PERIODS_SCORING
    },
    '781': {
        'title': 'Period ({replace1}) > Period ({replace2}) - No',
        'market': MARKET_PERIODS_SCORING
    },
    '796': {
        'title': 'First Team Win in End',
        'market': MARKET_END_RESULT
    },
    '797': {
        'title': 'Team 2 To Win End',
        'market': MARKET_END_RESULT
    },
    '798': {
        'title': 'Draw In End',
        'market': MARKET_END_RESULT
    },
    '835': {
        'title': 'Wins {} - Yes',
        'market': MARKET_WHO_WILL_WIN_YES_NO
    },
    '836': {
        'title': 'Wins {} - No',
        'market': MARKET_WHO_WILL_WIN_YES_NO
    },
    '841': {
        'title': 'Team 1 To Score In Each Period - Yes',
        'market': MARKET_TEAM_TO_SCORE_IN_EACH_PERIOD_YES_NO
    },
    '842': {
        'title': 'Team 1 To Score In Each Period - No',
        'market': MARKET_TEAM_TO_SCORE_IN_EACH_PERIOD_YES_NO
    },
    '843': {
        'title': 'Team 2 To Score In Each Period - Yes',
        'market': MARKET_TEAM_TO_SCORE_IN_EACH_PERIOD_YES_NO
    },
    '844': {
        'title': 'Team 2 To Score In Each Period - No',
        'market': MARKET_TEAM_TO_SCORE_IN_EACH_PERIOD_YES_NO
    },
    '881': {
        'title': '({replace1}) st/nd/rd End/Set ({replace2}) - Team 1 Win',
        'market': MARKET_END_WINNER
    },
    '882': {
        'title': '({replace1}) st/nd/rd End/Set ({replace2}) - Team 2 Win',
        'market': MARKET_END_WINNER
    },
    '971': {
        'title': 'Total Sets Over ({replace1})',
        'market': MARKET_TOTAL_SETS_OVER_UNDER
    },
    '972': {
        'title': 'Total Sets Under ({replace1})',
        'market': MARKET_TOTAL_SETS_OVER_UNDER
    },
    '981': {
        'title': 'Team 1 To Win In Overtime - Yes',
        'market': MARKET_OVERTIME_WIN
    },
    '982': {
        'title': 'Team 1 To Win In Overtime - No',
        'market': MARKET_OVERTIME_WIN
    },
    '1098': {
        'title': 'Penalty Shootout - Yes',
        'market': MARKET_PENALTY_SHOTS_YES_NO
    },
    '1099': {
        'title': 'Penalty Shootout - No',
        'market': MARKET_PENALTY_SHOTS_YES_NO
    },
    '1100': {
        'title': '({replace1}) Break - Player 1',
        'market': MARKET_BREAK
    },
    '1101': {
        'title': '({replace1}) Break - Player 2',
        'market': MARKET_BREAK
    },
    '1102': {
        'title': '({replace1}) Break - Neither',
        'market': MARKET_BREAK
    },
    '1143': {
        'title': 'Each Team Will Score Under ({replace1}) - Yes',
        'market': MARKET_TOTAL_EACH_TEAM_WILL_SCORE_UNDER_OVER
    },
    '1144': {
        'title': 'Each Team Will Score Under ({replace1}) - No',
        'market': MARKET_TOTAL_EACH_TEAM_WILL_SCORE_UNDER_OVER
    },
    '1145': {
        'title': 'Each Team Will Score Over ({replace1}) - Yes',
        'market': MARKET_TOTAL_EACH_TEAM_WILL_SCORE_UNDER_OVER
    },
    '1146': {
        'title': 'Each Team Will Score Over ({replace1}) - No',
        'market': MARKET_TOTAL_EACH_TEAM_WILL_SCORE_UNDER_OVER
    },
    '1238': {
        'title': 'Highest Scoring Period Total Under ({replace1})',
        'market': MARKET_TOTAL_SCORING_PERIOD
    },
    '1239': {
        'title': 'Highest Scoring Period Total Over ({replace1})',
        'market': MARKET_TOTAL_SCORING_PERIOD
    },
    '1240': {
        'title': 'Lowest Scoring Period Total Under ({replace1})',
        'market': MARKET_TOTAL_SCORING_PERIOD
    },
    '1241': {
        'title': 'Lowest Scoring Period Total Over ({replace1})',
        'market': MARKET_TOTAL_SCORING_PERIOD
    },
    '1333': {
        'title': 'Total Matches "Both To Score" Under ({replace1})',
        'market': MARKET_BOTH_TO_SCORE
    },
    '1334': {
        'title': 'Total Matches "Both To Score" Over ({replace1})',
        'market': MARKET_BOTH_TO_SCORE
    },
    '1643': {
        'title': 'HT-FT 1X/1X',
        'market': MARKET_HF_FT
    },
    '1644': {
        'title': 'HT-FT 1X/12',
        'market': MARKET_HF_FT
    },
    '1645': {
        'title': 'HT-FT 1X/X2',
        'market': MARKET_HF_FT
    },
    '1646': {
        'title': 'HT-FT 12/1X',
        'market': MARKET_HF_FT
    },
    '1647': {
        'title': 'HT-FT 12/12',
        'market': MARKET_HF_FT
    },
    '1648': {
        'title': 'HT-FT 12/X2',
        'market': MARKET_HF_FT
    },
    '1649': {
        'title': 'HT-FT X2/1X',
        'market': MARKET_HF_FT
    },
    '1650': {
        'title': 'HT-FT X2/12',
        'market': MARKET_HF_FT
    },
    '1651': {
        'title': 'HT-FT X2/X2',
        'market': MARKET_HF_FT
    },
    '1652': {
        'title': 'W1W1 And Total Over ({replace1}) - Yes',
        'market': MARKET_HF_FT_TOTAL
    },
    '1653': {
        'title': 'W1W1 And Total Over ({replace1}) - No',
        'market': MARKET_HF_FT_TOTAL
    },
    '1654': {
        'title': 'W1X And Total Over ({replace1}) - Yes',
        'market': MARKET_HF_FT_TOTAL
    },
    '1655': {
        'title': 'W1X And Total Over ({replace1}) - No',
        'market': MARKET_HF_FT_TOTAL
    },
    '1656': {
        'title': 'W1W2 And Total Over ({replace1}) - Yes',
        'market': MARKET_HF_FT_TOTAL
    },
    '1657': {
        'title': 'W1W2 And Total Over ({replace1}) - No',
        'market': MARKET_HF_FT_TOTAL
    },
    '1658': {
        'title': 'XW1 And Total Over ({replace1}) - Yes',
        'market': MARKET_HF_FT_TOTAL
    },
    '1659': {
        'title': 'XW1 And Total Over ({replace1}) - No',
        'market': MARKET_HF_FT_TOTAL
    },
    '1660': {
        'title': 'XX And Total Over ({replace1}) - Yes',
        'market': MARKET_HF_FT_TOTAL
    },
    '1661': {
        'title': 'XX And Total Over ({replace1}) - No',
        'market': MARKET_HF_FT_TOTAL
    },
    '1662': {
        'title': 'XW2 And Total Over ({replace1}) - Yes',
        'market': MARKET_HF_FT_TOTAL
    },
    '1663': {
        'title': 'XW2 And Total Over ({replace1}) - No',
        'market': MARKET_HF_FT_TOTAL
    },
    '1664': {
        'title': 'W2W1 And Total Over ({replace1}) - Yes',
        'market': MARKET_HF_FT_TOTAL
    },
    '1665': {
        'title': 'W2W1 And Total Over ({replace1}) - No',
        'market': MARKET_HF_FT_TOTAL
    },
    '1666': {
        'title': 'W2X And Total Over ({replace1}) - Yes',
        'market': MARKET_HF_FT_TOTAL
    },
    '1667': {
        'title': 'W2X And Total Over ({replace1}) - No',
        'market': MARKET_HF_FT_TOTAL
    },
    '1668': {
        'title': 'W2W2 And Total Over ({replace1}) - Yes',
        'market': MARKET_HF_FT_TOTAL
    },
    '1669': {
        'title': 'W2W2 And Total Over ({replace1}) - No',
        'market': MARKET_HF_FT_TOTAL
    },
    '1757': {
        'title': '1 Will Score ({replace1}) Or Less',
        'market': MARKET_CORRECT_SCORE
    },
    '1758': {
        'title': '2 Will Score ({replace1}) Or Less',
        'market': MARKET_CORRECT_SCORE
    },
    '1759': {
        'title': 'Team 1 Wins After ({replace1})',
        'market': MARKET_CORRECT_SCORE
    },
    '1760': {
        'title': 'Team 2 Wins After ({replace1})',
        'market': MARKET_CORRECT_SCORE
    },
    '1842': {
        'title': 'Driver Starting From Pole Position Wins - Yes',
        'market': MARKET_WINNER
    },
    '1843': {
        'title': 'Driver Starting From Pole Position Wins - No',
        'market': MARKET_WINNER
    },
    '1850': {
        'title': 'Total Frames Over ({replace1})',
        'market': MARKET_TOTAL_FRAMES
    },
    '1851': {
        'title': 'Total Frames Under ({replace1})',
        'market': MARKET_TOTAL_FRAMES
    },
    '1860': {
        'title': 'Player 1 To Win At Least ({replace1}) Set(s) - Yes',
        'market': MARKET_PLAYER_WINS_SET
    },
    '1861': {
        'title': 'Player 1 To Win At Least ({replace1}) Set(s) - No',
        'market': MARKET_PLAYER_WINS_SET
    },
    '1862': {
        'title': 'Player 2 To Win At Least ({replace1}) Set(s) - Yes',
        'market': MARKET_PLAYER_WINS_SET
    },
    '1863': {
        'title': 'Player 2 To Win At Least ({replace1}) Set(s) - No',
        'market': MARKET_PLAYER_WINS_SET
    },
    '1864': {
        'title': 'Game ({replace1}) 15:15 - Yes',
        'market': MARKET_SCORE_IN_GAME
    },
    '1865': {
        'title': 'Game ({replace1}) 15:15 - No',
        'market': MARKET_SCORE_IN_GAME
    },
    '1866': {
        'title': 'Game ({replace1}) 30:30 - Yes',
        'market': MARKET_SCORE_IN_GAME
    },
    '1867': {
        'title': 'Game ({replace1}) 30:30 - No',
        'market': MARKET_SCORE_IN_GAME
    },
    '1882': {
        'title': 'First Match Goal - From ({replace1}) To ({replace2}) Minute - Yes',
        'market': MARKET_FIRST_MATCH_GOAL
    },
    '1883': {
        'title': 'First Match Goal - From ({replace1}) To ({replace2}) Minute - No',
        'market': MARKET_FIRST_MATCH_GOAL
    },
    '1884': {
        'title': 'First Match Goal - From ({replace1}) Min Or Later - Yes',
        'market': MARKET_FIRST_MATCH_GOAL
    },
    '1885': {
        'title': 'First Match Goal - From ({replace1}) Min Or Later - No',
        'market': MARKET_FIRST_MATCH_GOAL
    },
    '1886': {
        'title': 'Player 1 To Win ({replace1}) And ({replace2}) Set',
        'market': MARKET_WHO_WILL_WIN_SETS
    },
    '1887': {
        'title': 'Player 2 To Win ({replace1}) And ({replace2}) Set',
        'market': MARKET_WHO_WILL_WIN_SETS
    },
    '1888': {
        'title': 'Both To Win 1 Set',
        'market': MARKET_WHO_WILL_WIN_SETS
    },
    '1961': {
        'title': 'Total Over ({replace1}) In ({replace2}) End',
        'market': MARKET_TOTAL_OVER_END
    },
    '1962': {
        'title': 'Total Under ({replace1}) In ({replace2}) End',
        'market': MARKET_TOTAL_OVER_END
    },
    '1963': {
        'title': 'Score ({replace1})-({replace2}) In ({replace3}) End',
        'market': MARKET_END_RESULT
    },
    '1964': {
        'title': 'Sets Score ({replace1})-({replace2})',
        'market': MARKET_SETS_SCORE
    },
    '1996': {
        'title': 'Number Of Goals Over ({replace1})',
        'market': MARKET_NUMBET_OF_GOALS
    },
    '1997': {
        'title': 'Number Of Goals Under ({replace1})',
        'market': MARKET_NUMBET_OF_GOALS
    },
    '1998': {
        'title': 'Maximum Total Shots In The Match Over ({replace1})',
        'market': MARKET_TOTAL_SHOTS_IN_MATCH
    },
    '1999': {
        'title': 'Maximum Total Shots In The Match Under ({replace1})',
        'market': MARKET_TOTAL_SHOTS_IN_MATCH
    },
    '2000': {
        'title': 'Minimum Total Shots In The Match Over ({replace1})',
        'market': MARKET_TOTAL_SHOTS_IN_MATCH
    },
    '2001': {
        'title': 'Minimum Total Shots In The Match Under ({replace1})',
        'market': MARKET_TOTAL_SHOTS_IN_MATCH
    },
    '2016': {
        'title': 'Most Goals Scored In A Match Over ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2017': {
        'title': 'Most Goals Scored In A Match Under ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2018': {
        'title': 'Fewest Goals Scored In A Match Over ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2019': {
        'title': 'Fewest Goals Scored In A Match Under ({replace1}) ',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2020': {
        'title': 'Most Goals Conceded In A Match Over ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2021': {
        'title': 'Most Goals Conceded In A Match Under ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2022': {
        'title': 'Most Goals Scored And Conceded In A Match Over ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2023': {
        'title': 'Most Goals Scored And Conceded In A Match Under ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2024': {
        'title': 'Total Goals Conceded During Short-Handed Period Over ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2025': {
        'title': 'Total Goals Conceded During Short-Handed Period Under ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2026': {
        'title': 'Total Goals Conceded During Powerplay Over ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2027': {
        'title': 'Total Goals Conceded During Powerplay Under ({replace1})',
        'market': MARKET_GOALS_SCORED_IN_A_MATCH
    },
    '2058': {
        'title': 'Team {} Driver To Win - Yes',
        'market': MARKET_DRIVER_TO_WIN
    },
    '2059': {
        'title': 'Team {} Driver To Win - No',
        'market': MARKET_DRIVER_TO_WIN
    },
    '2060': {
        'title': 'Correct Score ({replace1})-({replace2}) Or Better 1',
        'market': MARKET_CORRECT_SCORE
    },
    '2061': {
        'title': 'Knockdown - Yes',
        'market': MARKET_KNOCKDOWN
    },
    '2062': {
        'title': 'Knockdown - No',
        'market': MARKET_KNOCKDOWN
    },
    '2063': {
        'title': 'Correct Score ({replace1})-({replace2}) Or Better 2',
        'market': MARKET_CORRECT_SCORE
    },
    '2106': {
        'title': 'Team 1 Total Goals ({replace1}) Over',
        'market': MARKET_TEAM_TOTAL_GOALS
    },
    '2107': {
        'title': 'Team 1 Total Goals ({replace1}) Under',
        'market': MARKET_TEAM_TOTAL_GOALS
    },
    '2108': {
        'title': 'Team 2 Total Goals ({replace1}) Over',
        'market': MARKET_TEAM_TOTAL_GOALS
    },
    '2109': {
        'title': 'Team 2 Total Goals ({replace1}) Under',
        'market': MARKET_TEAM_TOTAL_GOALS
    },
    '2269': {
        'title': 'Each Team To Score Exactly ({replace1}) Goals — Yes',
        'market': MARKET_TOTAL_TEAM
    },
    '2270': {
        'title': 'Each Team To Score Exactly ({replace1}) Goals - No',
        'market': MARKET_TOTAL_TEAM
    },
    '2271': {
        'title': 'Each Team To Score ({replace1}) Goals Or More - Yes',
        'market': MARKET_TOTAL_TEAM
    },
    '2272': {
        'title': 'Each Team To Score ({replace1}) Goals Or More - No',
        'market': MARKET_TOTAL_TEAM
    },
    '2292': {
        'title': 'Fight To Go The Distance - Yes',
        'market': MARKET_FIGHT_TO_GO_THE_DISTANCE
    },
    '2293': {
        'title': 'Fight To Go The Distance - No',
        'market': MARKET_FIGHT_TO_GO_THE_DISTANCE
    },
    '2294': {
        'title': '({replace1}) Rounds Or More',
        'market': MARKET_TOTAL_ROUNDS
    },
    '2295': {
        'title': '({replace1}) Rounds Or Less',
        'market': MARKET_TOTAL_ROUNDS
    },
    '2506': {
        'title': 'Regular Time',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2507': {
        'title': 'Overtime',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2508': {
        'title': 'Shootouts',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2509': {
        'title': 'Extra Time',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2510': {
        'title': 'Penalty Shootout',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2519': {
        'title': 'In Overtime - Yes',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2520': {
        'title': 'In Overtime - No',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2521': {
        'title': 'In Shootouts - Yes',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2522': {
        'title': 'In Shootouts - No',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2543': {
        'title': 'In Regular Time - Yes',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '2544': {
        'title': 'In Regular Time - No',
        'market': MARKET_WHEN_THE_WINNER_WILL_BE_DETERMINED
    },
    '3827': {
        'title': 'Total Over ({replace1})',
        'market': MARKET_ASIAN_TOTAL
    },
    '3828': {
        'title': 'Total Under ({replace1})',
        'market': MARKET_ASIAN_TOTAL
    },
    '3829': {
        'title': 'Handiсap 1 ({replace1})',
        'market': MARKET_ASIAN_HANDICAP
    },
    '3830': {
        'title': 'Handiсap 2 ({replace1})',
        'market': MARKET_ASIAN_HANDICAP
    }
};

def str_repeat(str, multiplier):
    return str * multiplier

def filterTeams(sport, teams):
	a = 1

start_time = time.time()
timestamp = str(int(time.time()));
bookmaker_title = '1xbet'
queue_path = '../../../queues/Downloaders/' + bookmaker_title + '/'
queue_csv_path = queue_path + 'queue.csv';
queue_reader_path = queue_path + bookmaker_title + '/' + timestamp + '/';
event_feeds = []

print('Reading CSV: ' + queue_csv_path)
# Extract row from CSV and process it
if os.path.exists(queue_csv_path):
	with open(queue_csv_path, 'r') as file:
		reader = csv.reader(file, delimiter=';')
		for row in reader:
			# timestamp;sports;type;files(separated by comma)
			folder_path = queue_path + row[2] + '/' + row[0] + '/'
			if os.path.exists(folder_path):
				files = row[3].split(',')
				if len(files) > 0:
					for file in files:
						file_path = folder_path + file
						if os.path.exists(file_path):
							print('Processing ' + file)
							events = ijson.items(open(file_path, 'r', encoding="utf-8"), 'item');
							for event in events:
								try:
									bookmaker_event = BookmakerEvent.BookmakerEvent()
									event_sport = event.get('S')
									event_tournament = event.get('C')
									event_id = event.get('I')
									event_name = event.get('H') + ' vs ' + event.get('A')
									teams = []
									odds = []
									date = ''
									print(bookmaker_title + ' :: Processing API event: ' + event_name)

									# Get teams
									if event.get('A') and event.get('H'):
										team1 = BookmakerEventTeam.BookmakerEventTeam()
										team2 = BookmakerEventTeam.BookmakerEventTeam()

										team1.title = event.get('H')
										team1.local = True

										team2.title = event.get('A')
										team2.local = False

										#checkTeamMembers(event_sport, team1)
										#checkTeamMembers(event_sport, team2)

										teams = [team1, team2]

									# Parse date
									matches = re.search('\/Date\((\d+)\)\/', event.get('D'))

									if matches:
										timestamp_in_milliseconds = matches.group(1)
										timestamp_in_seconds = int(timestamp_in_milliseconds) / 1000
										datetime = datetime.fromtimestamp(timestamp_in_seconds)

										if datetime:
											date = datetime.strftime('%Y-%m-%d %H:%M:%S')

									filterTeams(event_sport, teams)

									bookmaker_event.event_id = event_id
									bookmaker_event.title = event_name
									bookmaker_event.tournament = event_tournament
									bookmaker_event.sport = event_sport
									bookmaker_event.date = date

									if event.get('EE'):
										for outcome in event.get('EE'):
											if outcome.get('T') and str(outcome.get('T')) in outcomes:
												market = outcomes[str(outcome.get('T'))]['market']
												found_at = -1
												outcome_title = outcomes[str(outcome.get('T'))]['title']

												i = 0
												for odd in odds:
													if odd.title == market:
														found_at = i
													i += 1

												replace1_pos = outcome_title.find('{replace1}')
												replace2_pos = outcome_title.find('{replace2}')

												if replace2_pos > -1:
													parts = str(outcome.get('P')).split('.')
													replace1 = parts[0]
													replace2 = replace1

													if len(parts) > 1:
														right_pad_times = 3
														final_replace2 = parts[1]

														if right_pad_times >= len(final_replace2):
															zeros_count = right_pad_times - len(final_replace2)
															final_replace2 = final_replace2 + str_repeat('0', zeros_count)
															final_replace2 = final_replace2.lstrip("0");
														else:
															left = final_replace2[0 : right_pad_times]
															right = final_replace2[right_pad_times:]
															final_replace2 = left + '.' + right

														replace2 = final_replace2

													outcome_title = outcome_title.replace('{replace1}', replace1)
													outcome_title = outcome_title.replace('{replace2}', replace2)
												elif replace1_pos > -1:
													outcome_title = outcome_title.replace('{replace1}', str(outcome.get('P')))

												bookmaker_odd_outcome = BookmakerOddOutcome.BookmakerOddOutcome()

												bookmaker_odd_outcome.title = outcome.get('O') if outcome.get('O') else outcome_title
												bookmaker_odd_outcome.decimal = outcome.get('C')

												if found_at > -1:
													odds[found_at].outcomes.append(bookmaker_odd_outcome)
												else:
													odd = BookmakerOdd.BookmakerOdd()

													odd.title = market
													odd.outcomes = [bookmaker_odd_outcome]

													odds.append(odd)

									bookmaker_event.odds = odds

									# Get teams from markets if array is empty
									if len(teams) == 0:
										for odd in odds:
											i = 0
											for outcome in odd.outcomes:
												team = BookmakerEventTeam.BookmakerEventTeam()

												team.title = event.get('H')
												team.local = i == 0

												#checkTeamMembers(event_sport, team)

												teams.append(team)
											break

									bookmaker_event.teams = teams

									bookmaker_updater.processEvent(bookmaker_event)
								except:
									print(bookmaker_title + ' :: Could not process event')


print("--- %s seconds ---" % (time.time() - start_time))