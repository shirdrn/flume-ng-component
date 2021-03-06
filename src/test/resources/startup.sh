cd /usr/local/flume
bin/flume-ng agent -c ./conf/ -f conf/flume-conf-activity(upstream).properties -Dflume.root.logger=INFO,DAILY -Dflume.log.dir=/tmp/flume/logs -Dflume.log.file=flume-activity.log -DappFlag=Activity -n a1 &
bin/flume-ng agent -c ./conf/ -f conf/flume-conf-activity(downstream).properties -Dflume.root.logger=INFO,DAILY -Dflume.log.dir=/data/flume/logs -Dflume.log.file=flume-activity.log -DappFlag=Activity -n a1 &


bin/flume-ng agent --conf ./conf --conf-file conf/flume-conf-rollingfile.properties -Dflume.root.logger=DEBUG,console -Dflume.log.dir=/data/flume -Dflume.log.file=flume-rolling.log -DappFlag=RollingFile --name a2