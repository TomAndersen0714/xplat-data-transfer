#!/bin/bash
# kill old program
ps -ef | grep 'cross_platform_pulsar_consumer' | grep -v 'grep' | sed -n '1p' | awk '{print $2}' | xargs kill
# sleep for closing
sleep 10s
# initiate a new one
nohup python3 ../src/cross_platform_pulsar_consumer_v3.py ../conf/tb_to_mini.json 1>/dev/null 2>&1 &