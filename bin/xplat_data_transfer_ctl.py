#!/usr/bin/python3
import argparse
import subprocess

from time import sleep

SOURCE_DIR = "../src/cross_platform_pulsar_consumer_v3.py"
CONF_DIR = "../conf/all_platform_to_tb.json"


def main():
    # create a argument parser
    args_parser = argparse.ArgumentParser(description='xplat-data-transfer controller.')

    # introducing arguments
    args_parser.add_argument("action", type=str, choices=['start', 'stop', 'restart'])
    args_parser.add_argument("-V", "--version", dest="version", action="version",
                             version="0.0.1")

    # parse the input args within 'sys.argv'
    args = args_parser.parse_args()

    # process the args
    action = args.action
    if action == "start":
        cmd = f"""ps -ef | grep '{SOURCE_DIR}' | grep -v 'grep' | sed -n '1p' | awk '{{print $2}}'"""
        # print(cmd)
        pid = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if pid:
            print(f"This is already a daemon thread [{pid}].")
        else:
            cmd = f"nohup python3 {SOURCE_DIR} {CONF_DIR} 1>/dev/null 2>nohup.out & "
            # print(cmd)
            subprocess.check_call(cmd, shell=True)

    elif action == "stop":
        cmd = f"""ps -ef | grep '{SOURCE_DIR}' | grep -v 'grep' | sed -n '1p' | awk '{{print $2}}'"""
        # print(cmd)
        pid = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if pid:
            print(f"Closing the daemon thread [{pid}]...")
            subprocess.check_call(f"kill {pid}", shell=True)
            sleep(10)
        else:
            print("There is no daemon thread.")

    elif action == "restart":
        cmd = f"""ps -ef | grep '{SOURCE_DIR}' | grep -v 'grep' | sed -n '1p' | awk '{{print $2}}'"""
        # print(cmd)
        pid = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if pid:
            print(f"Closing the daemon thread [{pid}]...")
            subprocess.check_call(f"kill {pid}", shell=True)
            sleep(10)

        print("Starting a new daemon thread...")
        cmd = f"nohup python3 {SOURCE_DIR} {CONF_DIR} 1>/dev/null 2>nohup.out & "
        # print(cmd)
        subprocess.check_call(cmd, shell=True)


if __name__ == '__main__':
    main()
