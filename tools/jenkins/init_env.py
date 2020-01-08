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

# Because of end-to-end-performance-test running in the cluster, so This init_env.py only contains flink on
# standalone env, doesn't contains the init environment of maven, java and jenkins. Defaultly maven, java and jenkins
# environment are ready in the cluster
#

import sys
from logger import logger
from utils import run_command


def init_standalone_env(host_list, dest_path):
    for host in host_list:
        cmd = "ssh %s \" rm -rf %s\"; scp -r %s %s:%s" % (host, dest_path, dest_path, host, dest_path)
        logger.info("init_standalone_env  cmd:%s" % cmd)
        run_command(cmd)


def get_host_list(slave_file):
    hostlist = []
    with open(slave_file) as file:
        for data in file:
            if not(data == "" or data.startswith("#")):
                hostlist.append(data.split("\n")[0])
    return hostlist


def package(flink_home):
    cmd = "cd %s; mvn clean install -B -U -DskipTests -Drat.skip=true -Dcheckstyle.skip=true " % flink_home
    status, output = run_command(cmd)
    print("package, status:%s, output:%s" % (status, output))
    if output.find("BUILD SUCCESS") > 0:
        print("package success")
        return True
    else:
        print("package error~~~")
        return False


def get_target(flink_code_path):
    print("flink_code_path:%s" % flink_code_path)
    cmd = "ls %s/flink-dist/target/flink-*-bin/ |grep -v tar.gz" % flink_code_path
    status, output = run_command(cmd)
    print("status:%s, output:%s" % (status, output))
    if status:
        target_file = output.split("\n")[0]
        print(target_file, "%s/flink-dist/target/%s-bin/%s" % (flink_code_path, target_file, target_file))
        return target_file, "%s/flink-dist/target/%s-bin/%s" % (flink_code_path, target_file, target_file)
    else:
        return "", ""


def update_conf_slaves(dest_path, slave_file):
    cmd = "cp %s %s/conf/" % (slave_file, dest_path)
    run_command(cmd)


def init_env(flink_code_path, slave_file, dest_path, am_seserver_dddress):
    package_result = package(flink_code_path)
    print("package_result:%s" % package_result)
    if not package_result:
        logger.error("package error")
        return 1, ""

    host_list = get_host_list(slave_file)
    flink_path, source_path = get_target(flink_code_path)
    print("flink_path:%s, source_path:%s" % (flink_path, source_path))
    if flink_path == "":
        return False, ""
    dest_path = "%s/%s" % (dest_path, flink_path)
    if source_path != "":
        update_conf_slaves(source_path, slave_file)
        if os.path.exists(dest_path):
            cmd = "rm -rf %s" % dest_path
            run_command(cmd)
        cmd = "cp -r %s %s" % (source_path, dest_path)
        run_command(cmd)
        cmd = "sed -i \"s/jobmanager.rpc.address.*/jobmanager.rpc.address: %s/\" " \
              "%s/conf/flink-conf.yaml" % (dest_path, am_seserver_dddress.split(":")[0])
        run_command(cmd)
        init_standalone_env(host_list, dest_path)

        return True, dest_path
    else:
        logger.error("find target file error")
        return False, ""


def usage():
    logger.info("python3 init_env.py flink_code_path, slaves_file, dest_path, am_seserver_dddress")


if __name__ == "__main__":
    if len(sys.argv) < 4:
        logger.error("The param's number must be larger than 4")
        usage()
        sys.exit(1)
    flink_code_path = sys.argv[1]
    slaves_file = sys.argv[2]
    dest_path = sys.argv[3]
    am_seserver_dddress = sys.argv[4]

    init_env(flink_code_path, slaves_file, dest_path, am_seserver_dddress)
