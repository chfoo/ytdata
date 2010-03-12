#!/bin/sh

echo -ne "GET /~chris/ytdata/?gen HTTP/1.0\r\n\r\n" | nc -q -1 127.0.0.1 80
