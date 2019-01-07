#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "highrisk_album.py" | grep -v 'xiaoshuo' | grep -v 'grep'`
result=$(echo $ps_out | grep "highrisk_album.py")
if [[ "$result" != "" ]];then
     echo "highrisk_album.py is running"
else
     echo "highrisk_album.py is not running"
     python highrisk_album.py &
fi

