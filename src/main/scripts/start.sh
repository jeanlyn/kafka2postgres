#!/usr/bin/env bash
set -e
PROJECT_PATH=$(cd `dirname $0`/..; pwd)
SPARK_HOME=/opt/software/spark-1.5.3-SNAPSHOT-bin-2.2.0
ARTIFACTID=kafka2postgres
VERSION=1.0
CONSUME_TOPIC=
APPLICATION_ARGUMENT=""
MAINCLASS="cn.com.gf.bdp.kafka2postgres"
APPNAME="cn.com.gf.bdp.kafka2postgres"

$SPARK_HOME/bin/runStreamingApp.sh --kafka ${CONSUME_TOPIC} \
  --master yarn-cluster \
  --name $APPNAME \
  --conf spark.streaming.receiver.maxRate=10000 \
  --conf spark.streaming.kafka.maxRatePerPartition=10000 \
  --class ${MAINCLASS} \
  --files ${PROJECT_PATH}/conf/spark.conf \
  $PROJECT_PATH/lib/${ARTIFACTID}-${VERSION}.jar ${APPLICATION_ARGUMENT}
