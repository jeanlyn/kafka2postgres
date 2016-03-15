#!/usr/bin/env bash
set -e

PROJECT_PATH=$(cd `dirname $0`; pwd)
APPNAME="cn.com.gf.bdp.kafka2postgres"
APP_ID=$(yarn application -list -appStates RUNNING |grep $APPNAME|head -n 1|awk '{print $1}')
if [ -z $APP_ID ];then
  echo "Does not has this APPNAME:${APPNAME}"
else
  yarn application -kill ${APP_ID}
fi

