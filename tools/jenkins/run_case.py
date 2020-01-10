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
import re

if sys.version_info < (3, 5):
    print("Python versions prior to 3.5 are not supported.")
    sys.exit(-1)

from logger import logger
from utils import run_command
from restapi_common import get_avg_qps_by_restful_interface, check_task_managers
from init_env import get_host_list


def start_server(flink_home):
    cmd = "bash %s/bin/start-cluster.sh" % flink_home
    status, output = run_command(cmd)
    print("status:%s, output:%s"%(status, output))
    if status and output.find("Exception") < 0:
        return True
    else:
        return False


def end_server(flink_home, try_num=3):
    end_num = 0
    while True:
        end_num = end_num + 1
        cmd = "bash %s/bin/stop-cluster.sh" % flink_home
        status, output = run_command(cmd)
        print("end-server status:%s, output:%s" % (status, output))
        cmd = "ps -aux|grep StandaloneSessionClusterEntrypoint|awk '{print $2}'|xargs kill -9 "
        run_command(cmd)
        if output.find("No taskexecutor daemon") >= 0:
            return True
        if end_num > try_num:
            return False


def get_scenarios(scenario_file_name, test_jar):
    """
    Parse file which contains several scenarios. Its content looks like the following examples.
    scenarioIndex   classPath scenarioName  jobparam1   jobparams2
    1 org.apache.Test testScenario1 aaa bbb
    ……
    :param scenario_file_name: scenario's file
    :param test_jar:
    :return: list of scenarios
    """
    params_name = []
    scenarios = []
    scenario_names = []
    line_num = 0
    with open(scenario_file_name) as file:
        for data in file:
            if (data.startswith("#") or data.startswith(" *") or data == "\n" or
                data.startswith("/*") or data.startswith(" */")):
                continue
            line = data.split("\n")[0]
            line_num = line_num + 1
            cmd = ""
            scenario_name = ""
            if line_num == 1:
                params_name = list(filter(None, line.split(" ")))
                print("params_name:%s"%params_name)
                if not ("testClassPath" in params_name):
                    return 1, [], []
            else:
                params_value = list(filter(None, line.split(" ")))
                print("params_value:%s"%params_value)
                for index in range(0, len(params_name)):
                    param = params_name[index]
                    value = params_value[index]
                    if param == "testClassPath":
                        cmd = "-c %s %s %s" % (value, test_jar, cmd)
                    else:
                        if cmd == "":
                            cmd = "--%s %s" % (param, value)
                        else:
                            cmd = "%s --%s %s" % (cmd, param, value)
                    if param != "testClassPath":
                        scenario_name = "%s_%s" % (scenario_name, value)
            if cmd != "":
                scenarios.append(cmd)
                scenario_names.append(scenario_name[1:])
    return 0, scenarios, scenario_names


def get_avg(values):
    if len(values) == 0:
        return 0.0
    else:
        return sum(values) * 1.0 / len(values)


def cancel_job(job_id, flink_home, am_seserver_dddress):
    cmd = "%s/bin/flink cancel -m %s %s " % (flink_home, am_seserver_dddress, job_id)
    status, output = run_command(cmd)
    if status:
        return True
    else:
        logger.error("stop server failed:%s" % output)
        return False


def cancel_all_job(flink_home, am_seserver_dddress):
    cmd = "bash %s/bin/flink list |grep RUNNING|awk '{print $4}'|xargs bash %s/bin/flink cancel -m %s "\
          % (flink_home, flink_home, am_seserver_dddress)
    run_command(cmd)


def get_job_id(output):
    regex_match = re.search("Job has been submitted with JobID ([a-z0-9]{32})", output)
    job_id = regex_match.group(1)
    return job_id


def env_check(flink_home, am_seserver_dddress, expect_task_managers_num):
    if not check_task_managers(am_seserver_dddress, expect_task_managers_num):
        end_server(flink_home)
        start_server(flink_home)
        cancel_all_job(flink_home, am_seserver_dddress)


def get_host_num(flink_home):
    host_file = "%s/conf/slaves"%flink_home
    host_list = get_host_list(host_file)
    return len(host_list)


def run_cases(scenario_file_name, flink_home, am_seserver_dddress, test_jar, result_file,
              inter_nums, wait_minute):
    end_result = end_server(flink_home)

    if not end_result:
        logger.info("end server failed")
    else:
        logger.info("end server success")

    status = start_server(flink_home)
    if not status:
        logger.info("start server failed")
    else:
        logger.info("start server success")
    status, scenarios, scenario_names = get_scenarios(scenario_file_name, test_jar)
    print("scenarios:%s" % scenarios)
    print("scenario_names:%s" % scenario_names)
    host_num = get_host_num(flink_home)
    for scenario_index in range(0, len(scenarios)):
        scenario = scenarios[scenario_index]
        scenario_name = scenario_names[scenario_index]
        print("scenario:%s,scenario_name:%s " % (scenario, scenario_name))
        total_qps = []
        for inter_index in range(0, inter_nums):
            env_check(flink_home, am_seserver_dddress, host_num)
            cmd = "bash %s/bin/flink run -d -m %s %s" % (flink_home, am_seserver_dddress, scenario)
            status, output = run_command(cmd, timeout=150)
            logger.info("inter_index:%s, status:%s, output:%s" % (inter_index, status, output))
            if status:
                time.sleep(120)
                job_id = get_job_id(output)
                print("job_id:%s" % job_id)
                for qps_index in range(0, 10):
                    qps = get_avg_qps_by_restful_interface(am_seserver_dddress, job_id)
                    total_qps.append(qps)
                    time.sleep(wait_minute)
                cancel_job(job_id, flink_home, am_seserver_dddress)
            else:
                logger.error("commit job failed")
                continue
        logger.info("total_qps:%s" % total_qps)
        avg_qps = get_avg(total_qps)
        logger.info("The avg qps of %s's  is %s" % (scenario_name, avg_qps))
        logger.info("type(avg_qps):%s, result_file:%s" % (type(avg_qps), result_file))
        result = open(result_file, "a")
        result.write("%s %s \n" % (scenario_name, avg_qps))
        result.close()
    end_server(flink_home)


def usage():
    logger.info("python3 run_case.py am_seserver_dddress scenario_file flink_home test_jar inter_nums wait_minute")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        logger.error("The param's number must be larger than 4")
        usage()
        sys.exit(1)
    am_seserver_dddress = sys.argv[1]
    scenario_file = sys.argv[2]
    flink_home = sys.argv[3]
    test_jar = sys.argv[4]
    result_file = sys.argv[5]
    inter_nums = 2
    if len(sys.argv) > 6:
        inter_nums = int(sys.argv[6])
    wait_minute = 10
    if len(sys.argv) > 7:
        wait_minute = int(sys.argv[7])
    print("scenario_file:%s, flink_home:%s, am_seserver_dddress:%s, test_jar:%s, result_file :%s,inter_nums:%s,  "
          "wait_minute:%s" % (scenario_file, flink_home, am_seserver_dddress, test_jar,
                              result_file, inter_nums, wait_minute))
    run_cases(scenario_file, flink_home, am_seserver_dddress, test_jar, result_file, inter_nums, wait_minute)
