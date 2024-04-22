import logging
import socket

class Stream:
    def __init__(self, socket):
        self._socket = socket
        addr = socket.getpeername()
        
    def send(self, message):
        """
        Sends a message to the client socket, followed by a newline character, preventing short-writes.
        """
        
        bytes_sent = 0
        bytes_to_send = len(message)
        while bytes_sent < bytes_to_send:
            send_result = self._socket.send(message[bytes_sent:])
            # Handle error in send
            if send_result == 0:
                raise OSError("Socket connection broken")
            bytes_sent += send_result
        
    def recv(self, bytes_to_receive):
        """
        Receives a message from the client socket, preventing short-reads.
        And returns the message received in bytes, must be decoded.
        """
        message = b""
        bytes_read = 0
        while bytes_read < bytes_to_receive:
            received = self._socket.recv(bytes_to_receive - bytes_read)
            # Handle error in receive
            if not received:
                raise OSError("Socket connection broken")
            message += received
            bytes_read += len(received)
        return message