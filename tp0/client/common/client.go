package common

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID            string
	ServerAddress string
	LoopLapse     time.Duration
	LoopPeriod    time.Duration
	BetsPerChunk  int
	CurrentPhase  int
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	conn   net.Conn
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	client := &Client{
		config: config,
	}
	return client
}

// CreateClientSocket Initializes client socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (c *Client) createClientSocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Fatalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

// Send a message to the server.
// This function avoids the short-write problem
func (c *Client) sendMessage(msg string) error {
	writer := bufio.NewWriter(c.conn)
	writer.WriteString(msg)
	err := writer.Flush()
	if err != nil {
		log.Fatalf("action: flush_message | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	return nil
}

// Receives a message from the server.
// This function avoids the short-read problem
func (c *Client) receiveMessage() (string, error) {
	msg, err := bufio.NewReader(c.conn).ReadString(DELIMITER[0])
	if err != nil {
		log.Debugf("action: receive_message | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	return msg, err
}

func (_c *Client) initializeSignalReceiver() chan os.Signal {
	signalReceiver := make(chan os.Signal, 1)
	signal.Notify(signalReceiver, syscall.SIGTERM)
	return signalReceiver
}

func (c *Client) handleShutdown(signalReceiver chan os.Signal) {
	c.conn.Close()
	log.Infof("action: socket_closing | result: success | client_id: %v",
		c.config.ID,
	)

	close(signalReceiver)
	log.Infof("action: signal_receiver_channel_shutdown | result: success | client_id: %v",
		c.config.ID,
	)
}

// StartClientLoop Send messages to the client until some time threshold is met
func (c *Client) StartClientLoop() {
	signalReceiver := c.initializeSignalReceiver()

	agencyFile, err := os.Open("agency-" + c.config.ID + ".csv")
	if err != nil {
		log.Fatalf("action: open_agency_file | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	defer agencyFile.Close()
	agencyFileReader := csv.NewReader(agencyFile)

	// autoincremental msgToSendID to identify every message sent
	msgToSendID := 1

	// Create the connection the server
	c.createClientSocket()
	defer c.conn.Close()

	for {

		select {
		case <-signalReceiver:
			c.handleShutdown(signalReceiver)
			return
		default:
		}

		shouldReturn := c.handleCommunicationWithServer(agencyFileReader, &msgToSendID)
		if shouldReturn {
			return
		}

	}

}

// Handles the communication with the server by reading the agency file and sending its contents in chunks.
// Returns a boolean indicating whether the caller should return due to an unexpected error such as a sudden connection closure.
func (c *Client) handleCommunicationWithServer(agencyFileReader *csv.Reader, msgToSendID *int) bool {
	if c.config.CurrentPhase == CHUNK_SENDING_PHASE {
		shouldReturn := c.handleChunkSendingPhase(agencyFileReader, msgToSendID)
		if shouldReturn {
			return true
		}
	} else if c.config.CurrentPhase == NOTIFYING_PHASE {
		shouldReturn := c.handleNotifyingPhase(msgToSendID)
		if shouldReturn {
			return true
		}
	} else if c.config.CurrentPhase == RESULTS_PHASE {
		shouldReturn := c.handleResultsPhase(msgToSendID)
		if shouldReturn {
			return true
		}
	}
	return false

}

// Handles the waiting phase of the client.
// It sends a result request message to the server and waits for them.
// Changes the client's phase to RESULTS_PHASE if the server sends the results, and if not, it sleeps for a while before returning.
// Returns a boolean indicating whether the caller should return, either because of an error or because the client finished all of its tasks.
func (c *Client) handleResultsPhase(msgToSendID *int) bool {
	agencyID, err := strconv.Atoi(c.config.ID)
	if err != nil {
		return true
	}
	log.Infof("action: results_phase_started | result: in_progress | client_id: %v",
		c.config.ID,
	)
	msgToSend := QUERY_RESULTS_MSG_HEADER + fmt.Sprintf(QUERY_RESULTS_MSG_FORMAT, agencyID) + DELIMITER
	err = c.sendMessage(msgToSend)
	if err != nil {
		return true
	}

	receivedMsg, err := c.receiveMessage()
	if err != nil {
		return true
	}
	receivedHeader := string(receivedMsg[0])

	if receivedHeader == RESULTS_MSG_HEADER_FROM_SV {
		winnersDocIDs := DecodeResultsMessageFromServer(receivedMsg)
		log.Infof("action: consulta_ganadores | result: success | client_id: %v | cant_ganadores: %d | dni_ganadores: %v",
			c.config.ID,
			len(winnersDocIDs),
			winnersDocIDs,
		)
		return true
	} else if receivedHeader == WAIT_MSG_HEADER_FROM_SV {
		time.Sleep(c.config.LoopPeriod)
	}
	(*msgToSendID)++
	return false
}

// Handles the notifying phase of the client.
// It sends a message to the server indicating that the client has finished sending all the bets.
// Returns a boolean indicating whether the caller should return due to an unexpected error such as a sudden connection closure.
func (c *Client) handleNotifyingPhase(msgToSendID *int) bool {
	agencyID, err := strconv.Atoi(c.config.ID)
	if err != nil {
		return true
	}
	msgToSend := NOTIFY_MSG_HEADER + fmt.Sprintf(NOTIFY_MSG_FORMAT, agencyID) + DELIMITER

	err = c.sendMessage(msgToSend)
	if err != nil {
		return true
	}

	receivedMsg, err := c.receiveMessage()
	if err != nil {
		return true
	}
	if receivedMsg == ACK_NOTIFY_MSG_FROM_SV {
		log.Infof("action: notify_ack_received | result: success | client_id: %v",
			c.config.ID,
		)
	} else {
		log.Errorf("action: message_mismatch | result: fail | client_id: %v | received_message: %v",
			c.config.ID,
			receivedMsg,
		)
		return true
	}

	log.Infof("action: notify_phase_completed | result: success | client_id: %v",
		c.config.ID,
	)
	c.config.CurrentPhase = RESULTS_PHASE

	(*msgToSendID)++
	return false
}

// Handles the chunk sending phase of the client.
// It reads the agency file and sends its contents in chunks to the server.
// Returns a boolean indicating whether the caller should return due to an unexpected error such as a sudden connection closure.
func (c *Client) handleChunkSendingPhase(agencyFileReader *csv.Reader, msgToSendID *int) bool {
	encodedBets, sizeOfCurrentChunk, amountOfBetsRead, reachedEOF := c.tryReadChunkOfBets(agencyFileReader)
	if reachedEOF && amountOfBetsRead == 0 {
		c.config.CurrentPhase = NOTIFYING_PHASE
		return false
	} else if reachedEOF && amountOfBetsRead > 0 {
		c.config.BetsPerChunk = amountOfBetsRead
	}

	if sizeOfCurrentChunk > MAX_CHUNK_SIZE_PER_MSG {
		shouldReturn := c.handleDeliveryOfExceedingChunks(amountOfBetsRead, encodedBets, msgToSendID)
		if shouldReturn {
			return true
		}
	} else {
		shouldReturn := c.handleDeliveryOfSingleChunk(encodedBets, msgToSendID)
		if shouldReturn {
			return true
		}
	}
	return false
}

// Handles the delivery of a single chunk when the amount of bets read made the current chunk fit in a single message.
// It sends the chunk to the server and waits for an acknowledgment.
// Returns a boolean indicating whether the caller should return due to an unexpected error such as a sudden connection closure.
func (c *Client) handleDeliveryOfSingleChunk(encodedBets []string, msgToSendID *int) bool {
	msgToSend := produceMsgToSendFromEncodedBets(c.config.BetsPerChunk, encodedBets)
	err := c.sendMessage(msgToSend)
	if err != nil {
		return true
	}

	receivedMsg, err := c.receiveMessage()
	if err != nil {
		return true
	}
	if receivedMsg == CHUNK_ACK_MSG_FROM_SV {
		log.Infof("action: chunk_ack_received | result: success | client_id: %v | chunk_id: %v",
			c.config.ID,
			*msgToSendID,
		)
	} else {
		log.Errorf("action: message_mismatch | result: fail | client_id: %v | chunk_id: %v | received_message: %v",
			c.config.ID,
			*msgToSendID,
			receivedMsg,
		)
		return true
	}

	(*msgToSendID)++
	return false
}

// Handles the delivery of chunks when the amount of bets read made the current chunk exceed the maximum size of a message.
// It divides the chunk into smaller chunks with the default amount of bets per chunk and sends them to the server.
// Returns a boolean indicating whether the caller should return due to an unexpected error such as a sudden connection closure.
func (c *Client) handleDeliveryOfExceedingChunks(amountOfBetsRead int, encodedBets []string, msgToSendID *int) bool {
	betsPerChunkCorrectlyAdjusted := false
	if amountOfBetsRead%DEFAULT_BETS_PER_CHUNK != 0 {
		amountOfBetsInFirstChunk := amountOfBetsRead % DEFAULT_BETS_PER_CHUNK
		c.config.BetsPerChunk = amountOfBetsInFirstChunk
	} else {
		c.config.BetsPerChunk = DEFAULT_BETS_PER_CHUNK
	}

	for len(encodedBets) > 0 {
		msgToSend := produceMsgToSendFromEncodedBets(c.config.BetsPerChunk, encodedBets)
		err := c.sendMessage(msgToSend)
		if err != nil {
			return true
		}

		receivedMsg, err := c.receiveMessage()
		if err != nil {
			return true
		}
		if receivedMsg == CHUNK_ACK_MSG_FROM_SV {
			log.Infof("action: chunk_ack_received | result: success | client_id: %v | chunk_id: %v",
				c.config.ID,
				*msgToSendID,
			)
		} else {
			log.Errorf("action: message_mismatch | result: fail | client_id: %v | chunk_id: %v | received_message: %v",
				c.config.ID,
				*msgToSendID,
				receivedMsg,
			)
			return true
		}

		encodedBets = encodedBets[c.config.BetsPerChunk:]
		if !betsPerChunkCorrectlyAdjusted {
			betsPerChunkCorrectlyAdjusted = true
			c.config.BetsPerChunk = DEFAULT_BETS_PER_CHUNK
		}

		(*msgToSendID)++
	}
	return false
}

func produceMsgToSendFromEncodedBets(betsPerChunk int, encodedBets []string) string {
	msgToSend := BETS_MSG_HEADER
	for i := 0; i < betsPerChunk; i++ {
		if i == betsPerChunk-1 {
			msgToSend += encodedBets[i] + DELIMITER
		} else {
			msgToSend += encodedBets[i] + ","
		}
	}
	return msgToSend
}

// Reads bets from the agency file up to its limit.
// This limit is reached when either:
// - The amount of bets read is equal to the maximum amount of bets per chunk defined in the client configuration
// - The size of the current chunk exceeds the maximum size of a message (8 KiB; though it still returns the list with the bets that could be read until that point in order to send them in smaller default-sized chunks later on)
// - The end of the file is reached
// Returns the list of bets read, the current size of the chunk, the amount of bets read and a boolean indicating whether the end of the file was reached
func (c *Client) tryReadChunkOfBets(agencyFileReader *csv.Reader) ([]string, int, int, bool) {
	encodedBets := []string{}
	sizeOfCurrentChunk := 0

	amountOfBetsRead := 0
	reachedEOF := false

	for (amountOfBetsRead < c.config.BetsPerChunk) && (sizeOfCurrentChunk < MAX_CHUNK_SIZE_PER_MSG) && !reachedEOF {
		recordAsBet, err := ReadBetFromCsvRecord(agencyFileReader, c.config.ID)
		if err != io.EOF && err != nil {
			log.Fatalf("action: read_agency_file | result: fail | client_id: %v | error: %v",
				c.config.ID,
				err,
			)
		}
		if err == io.EOF {
			reachedEOF = true
			continue
		}
		amountOfBetsRead++

		encodedBets = append(encodedBets, recordAsBet.ToString())
		sizeOfCurrentChunk += len(recordAsBet.ToString()) + 1 // Due to incoming separation commas
	}
	return encodedBets, sizeOfCurrentChunk, amountOfBetsRead, reachedEOF
}
