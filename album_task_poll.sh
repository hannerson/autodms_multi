#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep -v "xiaoshuo" | grep "album_task_poll.py" | grep -v 'grep'`
result=$(echo $ps_out | grep "album_task_poll.py")
if [[ "$result" != "" ]];then
     echo "album_task_poll.py is running"
else
     echo "album_task_poll.py is not running"
     python album_task_poll.py &
fi

