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

# This file will be runned by jenkis to run the e2e perf test
# Params:
# am_seserver_dddress: master machines's ip  of standalone environment
# scenario_file: the file which contains test scenarios
# flink_home: the path of flink
# inter_nums：the num of every scenario's running, default value is 10
# wait_minute: interval time of two elections of qps,default value is 10s
#


import sys
import time

if sys.version_info < (3, 5):
    print("Python versions prior to 3.5 are not supported.")
    sys.exit(-1)

from logger import logger
from utils import run_command
from restapi_common import get_avg_qps_by_restful_interface


def start_server(flink_home):
    cmd = "bash %s/bin/start_yarn.sh" % flink_home
    status, output = run_command(cmd)
    if status == 0:
        return 0
    else:
        return 1


def end_server(flink_home):
    cmd = "bash %s/bin/stop_yarn.sh" % flink_home
    run_command(cmd)


def get_scenarios(scenario_file_name, test_jar):
    """
    parser file which contains serval scenarios,it's content likes this:
    classPath       scenarioName      jobparam1     jobparams2
    org.apache.Test testScenario1     aaa           bbb
    ……
    :param scenario_file_name: scenario's file
    :param test_jar:
    :return: list of scenarios
    """
    scenario_file = open(scenario_file_name)
    line = scenario_file.readline()
    params_name = []
    scenarios = []
    scenario_names = []
    linenum = 0
    while line:
        linenum = linenum + 1
        cmd = ""
        scenario_name = ""
        if linenum == 1:
            params_name = line.split(" ")
            for index in range(0, len(params_name)):
                params_name[index] = params_name.get(index).trip()
            if not params_name.contains("className"):
                scenario_file.close()
                return 1, []
        else:
            params_value = line.split(" ")
            for index in len(params_name):
                param = params_name.get(index)
                if param == "className":
                    cmd = "-c %s %s %s" % (params_value.get(index),test_jar,  cmd)
                else:
                    cmd = "%s  --%s %s" % (cmd, param, params_value.get(index))
                scenario_name = "%s_%s" % (scenario_name, param)
        scenario_names.append(scenario_name[1:])
        scenarios.append(cmd)
        line = scenario_file.readline()
    scenario_file.close()
    return 0, scenarios, scenario_names


def get_avg(values):
    if len(values) == 0:
        return 0.0
    else:
        return sum(values) * 1.0 / len(values)


def run_cases(scenario_file_name, flink_home, am_seserver_dddress, inter_nums=10, wait_minute=10):
    status, scenarios, scenario_names = get_scenarios(scenario_file_name)
    for scenario_index in len(scenarios):
        scenario = scenarios.get(scenario_index)
        scenario_name = scenario_names.get(scenario_index)
        total_qps = []
        status = start_server(flink_home)
        if status != 0:
            logger.info("start server failed")
            return 1
        for inter_index in range(0, inter_nums):
            cmd = "bash %s/bin/flink run %s" % (flink_home, scenario)
            status, output = run_command(cmd)
            if status == 0:
                qps = get_avg_qps_by_restful_interface(am_seserver_dddress)
                total_qps.append(qps)
                time.sleep(wait_minute)
        avg_qps = get_avg(total_qps)
        logger.info("The avg qps of %s's  is %s" % (scenario_name, avg_qps))


if __name__ == "__main__":
    if len(sys.argv)<3:
        logger.error("The param's number must be larger than 3")
        sys.exit(1)
    am_seserver_dddress = sys.argv[1]
    scenario_file = sys.argv[2]
    flink_home = sys.argv[3]
    if len(sys.argv) >= 4:
        inter_nums = sys.argv[4]
    if len(sys.argv) >= 5:
        wait_minute = sys.argv[5]

    run_cases(scenario_file, flink_home, am_seserver_dddress, inter_nums=10, wait_minute=10)
