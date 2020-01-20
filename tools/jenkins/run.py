#!/usr/bin/env python
# -*- coding: utf-8 -*-
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# This file will be used in the task's config of jenkins,like this:
# python3 run.py ${WORKSPACE} slave_file dest_path result_file am_seserver_dddress inter_nums wait_minute

import argparse

from logger import logger
from init_env import init_env
from run_case import run_cases
from utils import run_command



def usage():
    logger.info("python3 run.py flink_code_path slave_file dest_path "
                "result_file am_seserver_dddress inter_nums wait_minute")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('flink_code_path',help='the path of flink code path downloaded')
    parser.add_argument('slaves_file', help='the slave\'s ips of the cluster')
    parser.add_argument('dest_path', help='flink install path')
    parser.add_argument('result_file', help='the result file')
    parser.add_argument('am_seserver_dddress', help='the master machine of cluster')
    parser.add_argument('--inter_nums', required=False, default=2, help='the nums of test job runs')
    parser.add_argument('--wait_minute', required=False, default=10, help='interval of two collections of metrics')
    args = parser.parse_args()
    flink_code_path = args.flink_code_path
    slaves_file = args.slaves_file
    dest_path = args.dest_path
    result_file = args.result_file
    am_seserver_dddress = args.am_seserver_dddress
    inter_nums = args.inter_nums
    wait_minute = args.wait_minute

    status, flink_home = init_env(flink_code_path, slaves_file, dest_path, am_seserver_dddress)
    if status:
        module = "flink-end-to-end-perf-tests/flink-basic-operations"
        status, output = run_command(
            "ls %s/%s/target/ |grep \"flink-basic-operations_.*.jar\" | grep -v test| grep -v original"
            % (flink_code_path, module))
        if status:
            test_jar = "%s/%s/target/%s" % (flink_code_path, module, output.split("\n")[0])
        scenarios_file = "%s/%s/src/main/resources/basic-operations-test-scenarios" % (flink_code_path, module)
        run_cases(scenarios_file, flink_home, am_seserver_dddress, test_jar, result_file, inter_nums, wait_minute)
    else:
        print("init error")
