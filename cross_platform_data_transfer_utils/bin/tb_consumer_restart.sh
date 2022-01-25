#!/bin/bash
# kill old one
ps -ef | grep 'cross_platform_pulsar_consumer_v3.py' | sed -n '1p' | awk '{print $2}' | xargs kill
# initiate a new one
nohup python3 ../src/cross_platform_pulsar_consumer_v3.py ../conf/all_platform_to_tb.json 1>/dev/null 2>&1 &