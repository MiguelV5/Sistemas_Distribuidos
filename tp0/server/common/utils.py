import csv
import datetime
import time


""" Bets storage location. """
STORAGE_FILEPATH = "./bets.csv"
""" Simulated winner number in the lottery contest. """
LOTTERY_WINNER_NUMBER = 7574

KiB = 1024
DELIMITER = b';'
DELIMITER_AS_STR = ";"
NEEDED_AGENCIES_TO_START_LOTTERY = 5

CHUNK_RECEIVED_MSG = "ACK_CHUNK;"
ACK_NOTIFY_MSG = "ACK_NOTIFY;"

BETS_MSG_HEADER_FROM_CL = "B"
NOTIFY_MSG_HEADER_FROM_CL = "N"
QUERY_RESULTS_MSG_HEADER_FROM_CL = "Q"
WAIT_MSG = "W;"
RESULTS_MSG_HEADER = "R"
RESULT_MSG_INNER_FORMAT = "PlayerDocID:{}"


""" A lottery bet registry. """
class Bet:
    def __init__(self, agency: str, first_name: str, last_name: str, document: str, birthdate: str, number: str):
        """
        agency must be passed with integer format.
        birthdate must be passed with format: 'YYYY-MM-DD'.
        number must be passed with integer format.
        """
        self.agency = int(agency)
        self.first_name = first_name
        self.last_name = last_name
        self.document = document
        self.birthdate = datetime.date.fromisoformat(birthdate)
        self.number = int(number)

""" Checks whether a bet won the prize or not. """
def has_won(bet: Bet) -> bool:
    return bet.number == LOTTERY_WINNER_NUMBER

"""
Persist the information of each bet in the STORAGE_FILEPATH file.
Not thread-safe/process-safe.
"""
def store_bets(bets: list[Bet]) -> None:
    with open(STORAGE_FILEPATH, 'a+') as file:
        writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
        for bet in bets:
            writer.writerow([bet.agency, bet.first_name, bet.last_name,
                             bet.document, bet.birthdate, bet.number])

"""
Loads the information all the bets in the STORAGE_FILEPATH file.
Not thread-safe/process-safe.
"""
def load_bets() -> list[Bet]:
    with open(STORAGE_FILEPATH, 'r') as file:
        reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            yield Bet(row[0], row[1], row[2], row[3], row[4], row[5])


def decode_bets(msg: str) -> list[Bet]:
    """
    Decodes a message with the format:
    "B{PlayerName:str,PlayerSurname:str,PlayerDocID:int,PlayerDateOfBirth:str,WageredNumber:int,AgencyID:int},...,{...};"
    """
    bets = []
   
    msg = msg[2:-2]  # remove the header, the first { amd the trailing };

    bet_entries = msg.split('},{')
    for bet_entry in bet_entries:
        bet = __decode_bet(bet_entry)
        bets.append(bet)

    return bets


def __decode_bet(msg: str) -> Bet:
    """
    Decodes a single bet entry with the format:
    "PlayerName:str,PlayerSurname:str,PlayerDocID:int,PlayerDateOfBirth:str,WageredNumber:int,AgencyID:int"
    """
    msg = msg.strip('{}')
    keys_and_values = msg.split(',')
    values = [kv.split(':')[1] for kv in keys_and_values]

    return Bet(values[5], values[0], values[1], values[2], values[3], values[4])


def decode_notify(msg: str) -> int:
    """
    Decodes a message with the format:
    "N{AgencyID:int};"
    """
    return __decode_agency(msg)

def decode_query_for_results(msg: str) -> int:
    """
    Decodes a message with the format:
    "Q{AgencyID:int};"
    """
    return __decode_agency(msg)

def __decode_agency(msg: str) -> int:
    return int(msg[11:-2])