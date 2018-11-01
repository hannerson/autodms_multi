#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "dispatcher_album.py" | grep -v 'grep'`
result=$(echo $ps_out | grep "dispatcher_album.py")
if [[ "$result" != "" ]];then
     echo "dispatcher_album.py is running"
else
     echo "dispatcher_album.py is not running"
     python dispatcher_album.py &
fi

