#!/bin/sh
if   [  $SERVER_PORT  ];
then
   java -Dserver.port=$SERVER_PORT -Xmx640m -Xms640m -jar /usr/local/src/app.jar &
else
   java -Dserver.port=8000 -jar /usr/local/src/app.jar &
fi
tail -f /usr/local/src/start.sh