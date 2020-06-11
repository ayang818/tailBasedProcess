#!/bin/sh
if   [  $SERVER_PORT  ];
then
   java -Dserver.port=$SERVER_PORT -Xmx2048m -Xms2048m -jar /usr/local/src/app.jar &
else
   java -Dserver.port=8000 -Xmx1024m -Xms1024m -jar /usr/local/src/app.jar &
fi
tail -f /usr/local/src/start.sh
