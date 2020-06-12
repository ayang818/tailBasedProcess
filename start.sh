#!/bin/sh
if   [  $SERVER_PORT = "8002" ];
then
   java -Dserver.port=$SERVER_PORT -Xmx1024m -Xms1024m -jar /usr/local/src/app.jar &
else
   java -Dserver.port=$SERVER_PORT -Xmx3072m -Xms3072m -jar /usr/local/src/app.jar &
fi
tail -f /usr/local/src/start.sh
