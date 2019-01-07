#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "highrisk_artist.py" | grep -v 'xiaoshuo' | grep -v 'grep'`
result=$(echo $ps_out | grep "highrisk_artist.py")
if [[ "$result" != "" ]];then
     echo "highrisk_artist.py is running"
else
     echo "highrisk_artist.py is not running"
     python highrisk_artist.py &
fi

