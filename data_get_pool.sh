#!/bin/bash
workdir=$(cd `dirname $0`;pwd)
cd $workdir
 
echo "current work direcory: $workdir"

ps_out=`ps -ef | grep "data_get_pool_utime.py" | grep -v 'grep'`
result=$(echo $ps_out | grep "data_get_pool_utime.py")
if [[ "$result" != "" ]];then
     echo "data_get_pool_utime.py is running"
else
     echo "data_get_pool_utime.py is not running"
     python data_get_pool_utime.py &
fi

ps_out=`ps -ef | grep "data_get_pool_utime_timing_online.py" | grep -v 'grep'`
result=$(echo $ps_out | grep "data_get_pool_utime_timing_online.py")
if [[ "$result" != "" ]];then
     echo "data_get_pool_utime_timing_online.py is running"
else
     echo "data_get_pool_utime_timing_online.py is not running"
     python data_get_pool_utime_timing_online.py &
fi

ps_out=`ps -ef | grep "data_get_pool_editor.py" | grep -v 'grep'`
result=$(echo $ps_out | grep "data_get_pool_editor.py")
if [[ "$result" != "" ]];then
     echo "data_get_pool_editor.py is running"
else
     echo "data_get_pool_editor.py is not running"
     python data_get_pool_editor.py &
fi
