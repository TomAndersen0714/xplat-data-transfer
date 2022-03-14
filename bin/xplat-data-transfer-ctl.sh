#!/bin/bash

if (($# != 1)); then
  echo -e "\nParameter Error!"
  exit 1
fi

SOURCE_DIR="../src/cross_platform_pulsar_consumer_v3.py"
CONF_DIR="../conf/tb_comsumer.json"

case $1 in
"start")
  nohup python3 ${SOURCE_DIR} ${CONF_DIR} 1>/dev/null 2>&1 &
  ;;

"restart")
  pid=$(ps -ef | grep 'cross_platform_pulsar_consumer_v3.py' | grep -v 'grep' | sed -n '1p' | awk '{print $2}')

  if [ "${pid}" != "" ]; then
    kill "${pid}"
    sleep 10s
  fi

  nohup python3 ${SOURCE_DIR} ${CONF_DIR} 1>/dev/null 2>&1 &
  ;;

"stop")
  pid=$(ps -ef | grep 'cross_platform_pulsar_consumer_v3.py' | grep -v 'grep' | sed -n '1p' | awk '{print $2}')

  if [ "${pid}" != "" ]; then
    kill "${pid}"
    sleep 10s
  fi
  ;;

*)
  echo -e "\nParameter Error!"
  exit 1
  ;;
esac
