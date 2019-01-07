#!/usr/bin/bash

hoursago=`date -d "30 minutes ago" "+%Y-%m-%d %H:%m:%d"`

echo $hoursago
mysql -h10.0.29.8 -uautodms -pyeelion -A AutoDMS -Ne"set names utf8;update MusicSrc set m_status=12 where m_status=1 and "
