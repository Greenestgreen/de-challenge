import os


class Constant():

    #Input Constants
    INPUT_RESULT = os.getenv('INPUT_RESULT', './data/result.csv')
    INPUT_CONSOLES = os.getenv('INPUT_CONSOLES', './data/consoles.csv')

    #Output Constants
    OUTPUT_FOLDER = os.getenv('OUTPUT_FOLDER', './report')
    TOP_10_CONSOLE_CSV = 'top10byconsole'
    WORST_10_CONSOLE_CSV = 'worst10byconsole'
    TOP_10_ALL = 'top10all'
    WROST_10_ALL = 'worst10all'
