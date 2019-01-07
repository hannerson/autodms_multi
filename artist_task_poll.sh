#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "artist_task_poll_tme.py" | grep -v 'xiaoshuo' | grep -v 'grep'`
result=$(echo $ps_out | grep "artist_task_poll_tme.py")
if [[ "$result" != "" ]];then
     echo "artist_task_poll_tme.py is running"
else
     echo "artist_task_poll_tme.py is not running"
     python artist_task_poll_tme.py &
fi

