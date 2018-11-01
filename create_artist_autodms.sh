workdir=$(cd `dirname $0`;pwd)

echo $workdir

cd $workdir

cur_date=`date +%Y%m%d%H%m`
mysql -h192.168.253.161 -uautodms -pyeelion -P 3306 -A AutoDMS -Ne"set names utf8;select m_artists,id from AlbumSrc where m_status in (0,2) and m_status_art=0 and m_artists!=\"\"" > artist/artist.album.$cur_date
mysql -h192.168.253.161 -uautodms -pyeelion -P 3306 -A AutoDMS -Ne"set names utf8;select m_artists,id from MusicSrc where m_status in (0,2) and m_status_art=0 and m_artists!=\"\"" > artist/artist.music.$cur_date

cat artist/artist.album.$cur_date artist/artist.music.$cur_date > artist/artist.$cur_date

python artist_split.py artist/artist.$cur_date $cur_date

#exit 0
python create_tx_artist.py $cur_date

if [[ $? -eq 0 ]];then
	echo "create_tx_artist ok"
else
	exit
fi

python update_artist_status.py artist/artist.album.$cur_date Album
python update_artist_status.py artist/artist.music.$cur_date Music
###update table status:{"default":0,"dispatch":1,"sig_ok":2,"artist_ok":3,"task_send":4,"artist_no":5,"task_fail":6,"task_suc":7,"sig_fail":8}
#mysql -h192.168.73.111 -umanualdms -pyeelion -A ManualDMS -Ne"set names utf8;update MusicSrc set m_status_art=3 where m_status_art=0 and batch_name=\"${1}\""
#mysql -h192.168.73.111 -umanualdms -pyeelion -A ManualDMS -Ne"set names utf8;update AlbumSrc set m_status_art=3 where m_status_art=0 and batch_name=\"${1}\""
