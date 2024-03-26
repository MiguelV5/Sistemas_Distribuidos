package common

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
)

const (
	DELIMITER = ";"

	BETS_MSG_HEADER        = "B"
	BET_MSG_FORMAT         = "{PlayerName:%s,PlayerSurname:%s,PlayerDocID:%d,PlayerDateOfBirth:%s,WageredNumber:%d,AgencyID:%d}"
	CHUNK_ACK_MSG_FROM_SV  = "ACK_CHUNK;"
	ACK_NOTIFY_MSG_FROM_SV = "ACK_NOTIFY;"

	NOTIFY_MSG_HEADER = "N"
	NOTIFY_MSG_FORMAT = "{AgencyID:%d}"

	QUERY_RESULTS_MSG_HEADER   = "Q"
	QUERY_RESULTS_MSG_FORMAT   = "{AgencyID:%d}"
	WAIT_MSG_HEADER_FROM_SV    = "W"
	RESULTS_MSG_HEADER_FROM_SV = "R"
	RESULTS_MSG_FORMAT_FROM_SV = "{PlayerDocID:%d}"

	DEFAULT_BETS_PER_CHUNK = 10
	KiB                    = 1024
	MAX_CHUNK_SIZE_PER_MSG = 8 * KiB

	CHUNK_SENDING_PHASE = 0
	NOTIFYING_PHASE     = 1
	RESULTS_PHASE       = 2
)

type Bet struct {
	PlayerName        string
	PlayerSurname     string
	PlayerDocID       int
	PlayerDateOfBirth string
	WageredNumber     int
	AgencyID          int
}

func (bet *Bet) ToString() string {
	return fmt.Sprintf(BET_MSG_FORMAT,
		bet.PlayerName,
		bet.PlayerSurname,
		bet.PlayerDocID,
		bet.PlayerDateOfBirth,
		bet.WageredNumber,
		bet.AgencyID,
	)
}

func ReadBetFromCsvRecord(agencyFileReader *csv.Reader, rawAgencyID string) (Bet, error) {
	record, err := agencyFileReader.Read()
	if err != nil {
		return Bet{}, err
	}

	playerDocID, err := strconv.Atoi(record[2])
	if err != nil {
		return Bet{}, err
	}
	wageredNumber, err := strconv.Atoi(record[4])
	if err != nil {
		return Bet{}, err
	}
	agencyID, err := strconv.Atoi(rawAgencyID)
	if err != nil {
		return Bet{}, err
	}

	recordAsBet := Bet{
		PlayerName:        record[0],
		PlayerSurname:     record[1],
		PlayerDocID:       playerDocID,
		PlayerDateOfBirth: record[3],
		WageredNumber:     wageredNumber,
		AgencyID:          agencyID,
	}

	return recordAsBet, nil

}

// Extracts the agencyID from the results message.
// In the case there are no winners, returns an empty slice of ints
func DecodeResultsMessageFromServer(resultsMsg string) []int {

	if resultsMsg == RESULTS_MSG_HEADER_FROM_SV+DELIMITER {
		return []int{}
	}

	resultsMsg = resultsMsg[1 : len(resultsMsg)-1] // Remove the header and the last delimiter
	playerDocIDs := make([]int, 0)

	playerIDEntries := strings.Split(resultsMsg, ",")
	for _, entry := range playerIDEntries {
		entry = strings.Trim(entry, "{PlayerDocID:}")
		playerDocID, _ := strconv.Atoi(entry)
		playerDocIDs = append(playerDocIDs, playerDocID)
	}

	return playerDocIDs

}
