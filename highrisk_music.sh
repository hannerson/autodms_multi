#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "highrisk_music.py" | grep -v 'xiaoshuo' | grep -v 'grep'`
result=$(echo $ps_out | grep "highrisk_music.py")
if [[ "$result" != "" ]];then
     echo "highrisk_music.py is running"
else
     echo "highrisk_music.py is not running"
     python highrisk_music.py &
fi

