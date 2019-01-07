#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "music_task_poll_single2.py" | grep -v 'xiaoshuo' | grep -v 'grep'`
result=$(echo $ps_out | grep "music_task_poll_single2.py")
if [[ "$result" != "" ]];then
     echo "music_task_poll_single2.py is running"
else
     echo "music_task_poll_single2.py is not running"
     python music_task_poll_single2.py &
fi

