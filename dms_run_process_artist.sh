#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "dispatcher_artist.py" | grep -v 'grep'`
result=$(echo $ps_out | grep "dispatcher_artist.py")
if [[ "$result" != "" ]];then
     echo "dispatcher_artist.py is running"
else
     echo "dispatcher_artist.py is not running"
     python dispatcher_artist.py &
fi

