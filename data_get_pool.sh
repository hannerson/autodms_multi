#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "data_get_pool.py" | grep -v 'grep'`
result=$(echo $ps_out | grep "data_get_pool.py")
if [[ "$result" != "" ]];then
     echo "data_get_pool.py is running"
else
     echo "data_get_pool.py is not running"
     python data_get_pool.py &
fi

