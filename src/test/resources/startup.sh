cd /usr/local/flume
bin/flume-ng agent -c ./conf/ -f conf/flume-conf-activity.properties -Dflume.root.logger=INFO,DAILY -Dflume.log.dir=/tmp/flume/logs -Dflume.log.file=flume-activity.log -DappFlag=Activity -n a1 &
bin/flume-ng agent -c ./conf/ -f conf/flume-conf-activity.properties -Dflume.root.logger=INFO,DAILY -Dflume.log.dir=/data/flume/logs -Dflume.log.file=flume-activity.log -DappFlag=Activity -n a1 &
