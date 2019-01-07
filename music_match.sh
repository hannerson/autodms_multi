#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "match_music.py" | grep -v 'grep'`
result=$(echo $ps_out | grep "match_music.py")
if [[ "$result" != "" ]];then
     echo "match_music.py is running"
else
     echo "match_music.py is not running"
     python match_music.py &
fi

