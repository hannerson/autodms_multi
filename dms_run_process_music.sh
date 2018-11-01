#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "dispatcher_music.py" | grep -v 'grep'`
result=$(echo $ps_out | grep "dispatcher_music.py")
if [[ "$result" != "" ]];then
     echo "dispatcher_music.py is running"
else
     echo "dispatcher_music.py is not running"
     python dispatcher_music.py &
fi

