#!/bin/bash
# kill old one
ps -ef | grep 'cross_platform_pulsar_consumer_v3.py' | grep -v 'grep' | sed -n '1p' | awk '{print $2}' | xargs kill
# sleep for closing
sleep 10s
# initiate a new one
nohup python3 ../src/cross_platform_pulsar_consumer_v3.py ../conf/tb_comsumer.json 1>/dev/null 2>&1 &