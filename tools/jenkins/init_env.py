#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Because of end-to-end-performance-test running in the cluster, so This init_env.py only contains flink on
# standalone env, doesn't contains the init environment of maven, java and jenkins. Defaultly maven, java and jenkins
# environment are ready in the cluster
#
# Note by AiHua Li

import os
from utils import run_command


def init_standalone_env(host_list, user, source_path, dest_path):
    for host in host_list:
        cmd = "sudo su %s -c 'ssh %s \" rm -rf %s\"; scp -r %s %s@%s:%s'" % (user, host, dest_path, source_path,
                                                                             user, host, dest_path)
        print "init_standalone_env  cmd:%s" % cmd
        os.system(cmd)


def get_host_list(slave_file):
    hostlist = []
    slavefile = open(slave_file, "r")
    line = slavefile.readline()
    while line:
        hostlist.append(line)
        line = slavefile.readline()
    slavefile.close()
    return hostlist


def package(flink_home):
    cmd = "cd %s; mvn clean install -B -U -DskipTests -Drat.skip=true -Dcheckstyle.skip=true --settings " \
          "~/.m2/settings_open.xml " % flink_home
    print cmd
    status, output = run_command(cmd)
    print output
    if output.find("BUILD SUCCESS")>0:
        return 0
    else:
        return 1


def get_target(flink_home):
    cmd = "ls -lt %s/flink-dist/target/flink-*-bin/ |grep -v tar.gz"
    status, output = run_command(cmd)
    if status == 0:
        target_file = output.split("\n")
        return target_file, "%s/flink-dist/target/%s-bin/%s" % (flink_home, target_file, target_file)
    else:
        return "", ""


def update_conf_slaves(dest_path, slave_file):
    cmd = "cp %s %s/conf/" % (slave_file, dest_path)
    run_command(cmd)

def init_env():
    flink_home = os.getcwd()
    package_result = package(flink_home)
    if package_result == 1:
        print "package error"
        return 1
    slave_file = "%s/tool/jenkins/slaves" % flink_home
    host_list = get_host_list(slave_file)
    flink_path, source_path = get_target(flink_home)
    dest_path = "/home/admin/%s" % flink_path
    if source_path != "":
        update_conf_slaves(source_path, slave_file)
        init_standalone_env(host_list, source_path, dest_path)
        return 0
    else:
        print "find target file error"
        return 1


if __name__ == "__main__":
    init_env()
