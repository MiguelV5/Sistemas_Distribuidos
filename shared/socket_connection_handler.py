import logging
from shared.stream import Stream

class SocketConnectionHandler:
    
    def __init__(self, sock):
        self._stream = Stream(sock)
        
    def send_message(self, message: str):
        """
        Sends a message to the client stream. With the lenght of the message in the first 4 bytes.
        """
        message = message.encode('utf-8')
        size_of_message = len(message)
        logging.info(f"action: send_message | result: in_progress | size: {size_of_message}")
        self._stream.send(int(size_of_message).to_bytes(4, byteorder='big'))
        self._stream.send(message)
        
        
    
    def read_message(self):
        """
        Reads a message from the client stream.
        """
        logging.info("action: read_message_size | result: in_progress")
        try: 
            size_of_message = int.from_bytes(self._stream.recv(4), byteorder='big')
            logging.debug(f"action: read_message_size | result: success | size: {size_of_message}")
            message = self._stream.recv(size_of_message).decode('utf-8')
        except OSError as e:
            logging.error(f"action: read_message_size | result: fail | error: {e}")
            raise OSError("Socket connection broken")
        return message
        