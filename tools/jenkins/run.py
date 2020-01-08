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

import sys
from logger import logger
from init_env import init_env
from run_case import run_cases
from utils import run_command


def usage():
    logger.info("python3 run.py flink_code_path slave_file dest_path "
                "result_file am_seserver_dddress inter_nums wait_minute")


if __name__ == "__main__":
    if len(sys.argv) < 5:
        logger.error("The param's number must be larger than 5")
        usage()
        sys.exit(1)

    flink_code_path = sys.argv[1]
    slave_file = sys.argv[2]
    dest_path = sys.argv[3]
    result_file = sys.argv[4]
    am_seserver_dddress = sys.argv[5]
    inter_nums = 2
    if len(sys.argv) > 6:
        inter_nums = int(sys.argv[6])
    wait_minute = 10
    if len(sys.argv) > 7:
        wait_minute = int(sys.argv[7])

    status, flink_home = init_env(flink_code_path, slave_file, dest_path, am_seserver_dddress)
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
