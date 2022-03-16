#!/usr/bin/python3
import argparse
import subprocess
import os

from sys import stderr
from time import sleep

MAIN_PATH = "../src/cross_platform_pulsar_consumer_v3.py"
CONF_PATH = "../conf/tb_comsumer.json"


def main():
    # create a argument parser
    args_parser = argparse.ArgumentParser(description='xplat-data-transfer controller.')

    # introducing arguments
    args_parser.add_argument("action", type=str, choices=['start', 'stop', 'restart', 'status'])
    args_parser.add_argument("-m", "--main", dest="main_path", type=str,
                             help="specify the execution file path")
    args_parser.add_argument("-c", "--conf", dest="conf_path", type=str,
                             help="specify the configuration file path")
    args_parser.add_argument("-v", "--version", dest="version", action="version",
                             version="0.0.1")

    # parse the input args within 'sys.argv'
    args = args_parser.parse_args()

    # process the args
    main_path = args.main_path if args.main_path else MAIN_PATH
    conf_path = args.conf_path if args.conf_path else CONF_PATH
    action = args.action

    # check the existence of path
    if not os.path.exists(main_path):
        print(f"{main_path} does not exists!")
    if not os.path.exists(conf_path):
        print(f"{conf_path} does not exists!")

    if action == "start":
        cmd = f"""ps -ef | grep '{main_path}' | grep -v 'grep' | sed -n '1p' | awk '{{print $2}}'"""
        # print(cmd)
        pid = subprocess.check_output(cmd, shell=True, stderr=stderr).decode('utf-8').strip()
        if pid:
            print(f"This is already a daemon thread [{pid}].")
            return
        else:
            cmd = f"nohup python3 {main_path} {conf_path} 1>/dev/null 2>&1 &"
            # print(cmd)
            output = subprocess.check_output(cmd, shell=True, stderr=stderr).decode('utf-8').strip()
            print(output)
            sleep(3)

        # get the status of daemon
        cmd = f"""ps -ef | grep '{main_path}' | grep -v 'grep' | sed -n '1p' | awk '{{print $2}}'"""
        # print(cmd)
        pid = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if pid:
            print(f"Daemon thread is running as [{pid}]")
        else:
            print("Daemon thread in not running.")

    elif action == "stop":
        cmd = f"""ps -ef | grep '{main_path}' | grep -v 'grep' | sed -n '1p' | awk '{{print $2}}'"""
        # print(cmd)
        pid = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if pid:
            print(f"Closing the daemon thread [{pid}]...")
            subprocess.check_call(f"kill {pid}", shell=True)
            sleep(10)
        else:
            print("Daemon thread in not running.")


    elif action == "restart":
        cmd = f"""ps -ef | grep '{main_path}' | grep -v 'grep' | sed -n '1p' | awk '{{print $2}}'"""
        # print(cmd)
        pid = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if pid:
            print(f"Closing the daemon thread [{pid}]...")
            subprocess.check_call(f"kill {pid}", shell=True)
            sleep(10)

        print("Starting a new daemon thread...")
        cmd = f"nohup python3 {main_path} {conf_path} 1>/dev/null 2>&1 &"
        # print(cmd)
        subprocess.check_call(cmd, shell=True)

        # get the status of daemon
        cmd = f"""ps -ef | grep '{main_path}' | grep -v 'grep' | sed -n '1p' | awk '{{print $2}}'"""
        # print(cmd)
        pid = subprocess.check_output(cmd, shell=True).decode('utf-8').strip()
        if pid:
            print(f"Daemon thread is running as [{pid}]")
        else:
            print("Daemon thread in not running.")

    elif action == "status":
        cmd = f"""ps -ef | grep '{main_path}' | grep -v 'grep' | sed -n '1p' | awk '{{print $2}}'"""
        # print(cmd)
        pid = subprocess.check_output(cmd, shell=True, ).decode('utf-8').strip()
        if pid:
            print(f"Daemon thread is running as [{pid}]")
        else:
            print("Daemon thread in not running.")


if __name__ == '__main__':
    main()
