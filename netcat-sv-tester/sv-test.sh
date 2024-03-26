#!/bin/sh

SV_HOST="server"
SV_PORT=12345

MSG="<<<TEST-MESSAGE>>>"
RECEIVED_REPLY=$(echo $MSG | nc $SV_HOST $SV_PORT)

if [ $RECEIVED_REPLY == $MSG ]; then
    echo
    echo EchoServer working correctly!
else
    echo
    echo "ERR: Wrong EchoServer response: Expected: $MSG; Got netcat output: $RECEIVED_REPLY"
fi
