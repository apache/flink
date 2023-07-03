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
import argparse
import getpass
import glob
import os
import platform
import signal
import socket
import sys
from collections import namedtuple
from string import Template
from subprocess import Popen, PIPE

from pyflink.find_flink_home import _find_flink_home, _find_flink_source_root

KEY_ENV_LOG_DIR = "env.log.dir"
KEY_ENV_YARN_CONF_DIR = "env.yarn.conf.dir"
KEY_ENV_HADOOP_CONF_DIR = "env.hadoop.conf.dir"
KEY_ENV_HBASE_CONF_DIR = "env.hbase.conf.dir"
KEY_ENV_JAVA_HOME = "env.java.home"
KEY_ENV_JAVA_OPTS = "env.java.opts.all"
KEY_ENV_JAVA_OPTS_DEPRECATED = "env.java.opts"


def on_windows():
    return platform.system() == "Windows"


def read_from_config(key, default_value, flink_conf_file):
    value = default_value
    # get the realpath of tainted path value to avoid CWE22 problem that constructs a path or URI
    # using the tainted value and might allow an attacker to access, modify, or test the existence
    # of critical or sensitive files.
    with open(os.path.realpath(flink_conf_file), "r") as f:
        while True:
            line = f.readline()
            if not line:
                break
            if line.startswith("#") or len(line.strip()) == 0:
                continue
            k, v = line.split(":", 1)
            if k.strip() == key:
                value = v.strip()
    return value


def find_java_executable():
    java_executable = "java.exe" if on_windows() else "java"
    flink_home = _find_flink_home()
    flink_conf_file = os.path.join(flink_home, "conf", "flink-conf.yaml")
    java_home = read_from_config(KEY_ENV_JAVA_HOME, None, flink_conf_file)

    if java_home is None and "JAVA_HOME" in os.environ:
        java_home = os.environ["JAVA_HOME"]

    if java_home is not None:
        java_executable = os.path.join(java_home, "bin", java_executable)

    return java_executable


def prepare_environment_variables(env):
    flink_home = _find_flink_home()
    # get the realpath of tainted path value to avoid CWE22 problem that constructs a path or URI
    # using the tainted value and might allow an attacker to access, modify, or test the existence
    # of critical or sensitive files.
    real_flink_home = os.path.realpath(flink_home)

    if 'FLINK_CONF_DIR' in env:
        flink_conf_directory = os.path.realpath(env['FLINK_CONF_DIR'])
    else:
        flink_conf_directory = os.path.join(real_flink_home, "conf")
    env['FLINK_CONF_DIR'] = flink_conf_directory

    if 'FLINK_LIB_DIR' in env:
        flink_lib_directory = os.path.realpath(env['FLINK_LIB_DIR'])
    else:
        flink_lib_directory = os.path.join(real_flink_home, "lib")
    env['FLINK_LIB_DIR'] = flink_lib_directory

    if 'FLINK_OPT_DIR' in env:
        flink_opt_directory = os.path.realpath(env['FLINK_OPT_DIR'])
    else:
        flink_opt_directory = os.path.join(real_flink_home, "opt")
    env['FLINK_OPT_DIR'] = flink_opt_directory

    if 'FLINK_PLUGINS_DIR' in env:
        flink_plugins_directory = os.path.realpath(env['FLINK_PLUGINS_DIR'])
    else:
        flink_plugins_directory = os.path.join(real_flink_home, "plugins")
    env['FLINK_PLUGINS_DIR'] = flink_plugins_directory

    env["FLINK_BIN_DIR"] = os.path.join(real_flink_home, "bin")


def construct_log_settings(env):
    templates = [
        "-Dlog.file=${flink_log_dir}/flink-${flink_ident_string}-python-${hostname}.log",
        "-Dlog4j.configuration=${log4j_properties}",
        "-Dlog4j.configurationFile=${log4j_properties}",
        "-Dlogback.configurationFile=${logback_xml}"
    ]

    flink_home = os.path.realpath(_find_flink_home())
    flink_conf_dir = env['FLINK_CONF_DIR']
    flink_conf_file = os.path.join(env['FLINK_CONF_DIR'], "flink-conf.yaml")

    if "FLINK_LOG_DIR" in env:
        flink_log_dir = env["FLINK_LOG_DIR"]
    else:
        flink_log_dir = read_from_config(
            KEY_ENV_LOG_DIR, os.path.join(flink_home, "log"), flink_conf_file)

    if "LOG4J_PROPERTIES" in env:
        log4j_properties = env["LOG4J_PROPERTIES"]
    else:
        log4j_properties = "%s/log4j-cli.properties" % flink_conf_dir

    if "LOGBACK_XML" in env:
        logback_xml = env["LOGBACK_XML"]
    else:
        logback_xml = "%s/logback.xml" % flink_conf_dir

    if "FLINK_IDENT_STRING" in env:
        flink_ident_string = env["FLINK_IDENT_STRING"]
    else:
        flink_ident_string = getpass.getuser()

    hostname = socket.gethostname()
    log_settings = []
    for template in templates:
        log_settings.append(Template(template).substitute(
            log4j_properties=log4j_properties,
            logback_xml=logback_xml,
            flink_log_dir=flink_log_dir,
            flink_ident_string=flink_ident_string,
            hostname=hostname))
    return log_settings


def get_jvm_opts(env):
    flink_conf_file = os.path.join(env['FLINK_CONF_DIR'], "flink-conf.yaml")
    jvm_opts = env.get(
        'FLINK_ENV_JAVA_OPTS',
        read_from_config(
            KEY_ENV_JAVA_OPTS,
            read_from_config(KEY_ENV_JAVA_OPTS_DEPRECATED, "", flink_conf_file),
            flink_conf_file))

    # Remove leading and ending double quotes (if present) of value
    jvm_opts = jvm_opts.strip("\"")
    return jvm_opts.split(" ")


def construct_flink_classpath(env):
    flink_home = _find_flink_home()
    flink_lib_directory = env['FLINK_LIB_DIR']
    flink_opt_directory = env['FLINK_OPT_DIR']

    if on_windows():
        # The command length is limited on Windows. To avoid the problem we should shorten the
        # command length as much as possible.
        lib_jars = os.path.join(flink_lib_directory, "*")
    else:
        lib_jars = os.pathsep.join(glob.glob(os.path.join(flink_lib_directory, "*.jar")))

    flink_python_jars = glob.glob(os.path.join(flink_opt_directory, "flink-python*.jar"))
    if len(flink_python_jars) < 1:
        print("The flink-python jar is not found in the opt folder of the FLINK_HOME: %s" %
              flink_home)
        return lib_jars
    flink_python_jar = flink_python_jars[0]

    return os.pathsep.join([lib_jars, flink_python_jar])


def construct_hadoop_classpath(env):
    flink_conf_file = os.path.join(env['FLINK_CONF_DIR'], "flink-conf.yaml")

    hadoop_conf_dir = ""
    if 'HADOOP_CONF_DIR' not in env and 'HADOOP_CLASSPATH' not in env:
        if os.path.isdir("/etc/hadoop/conf"):
            print("Setting HADOOP_CONF_DIR=/etc/hadoop/conf because no HADOOP_CONF_DIR or"
                  "HADOOP_CLASSPATH was set.")
            hadoop_conf_dir = "/etc/hadoop/conf"

    hbase_conf_dir = ""
    if 'HBASE_CONF_DIR' not in env:
        if os.path.isdir("/etc/hbase/conf"):
            print("Setting HBASE_CONF_DIR=/etc/hbase/conf because no HBASE_CONF_DIR was set.")
            hbase_conf_dir = "/etc/hbase/conf"

    return os.pathsep.join(
        [env.get("HADOOP_CLASSPATH", ""),
         env.get("YARN_CONF_DIR",
                 read_from_config(KEY_ENV_YARN_CONF_DIR, "", flink_conf_file)),
         env.get("HADOOP_CONF_DIR",
                 read_from_config(KEY_ENV_HADOOP_CONF_DIR, hadoop_conf_dir, flink_conf_file)),
         env.get("HBASE_CONF_DIR",
                 read_from_config(KEY_ENV_HBASE_CONF_DIR, hbase_conf_dir, flink_conf_file))])


def construct_test_classpath():
    test_jar_patterns = [
        "flink-python/target/test-dependencies/*",
        "flink-python/target/artifacts/testDataStream.jar",
        "flink-python/target/flink-python*-tests.jar",
    ]
    test_jars = []
    flink_source_root = _find_flink_source_root()
    for pattern in test_jar_patterns:
        pattern = pattern.replace("/", os.path.sep)
        test_jars += glob.glob(os.path.join(flink_source_root, pattern))
    return os.path.pathsep.join(test_jars)


def construct_program_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--class", required=True)
    parser.add_argument("cluster_type", choices=["local", "remote", "yarn"])
    parse_result, other_args = parser.parse_known_args(args)
    main_class = getattr(parse_result, "class")
    cluster_type = parse_result.cluster_type
    return namedtuple(
        "ProgramArgs", ["main_class", "cluster_type", "other_args"])(
        main_class, cluster_type, other_args)


def launch_gateway_server_process(env, args):
    prepare_environment_variables(env)
    program_args = construct_program_args(args)
    if program_args.cluster_type == "local":
        java_executable = find_java_executable()
        log_settings = construct_log_settings(env)
        jvm_args = env.get('JVM_ARGS', '')
        jvm_opts = get_jvm_opts(env)
        classpath = os.pathsep.join(
            [construct_flink_classpath(env), construct_hadoop_classpath(env)])
        if "FLINK_TESTING" in env:
            classpath = os.pathsep.join([classpath, construct_test_classpath()])
        command = [java_executable, jvm_args, "-XX:+IgnoreUnrecognizedVMOptions",
                   "--add-opens=jdk.proxy2/jdk.proxy2=ALL-UNNAMED"] \
            + jvm_opts + log_settings \
            + ["-cp", classpath, program_args.main_class] + program_args.other_args
    else:
        command = [os.path.join(env["FLINK_BIN_DIR"], "flink"), "run"] + program_args.other_args \
            + ["-c", program_args.main_class]
    preexec_fn = None
    if not on_windows():
        def preexec_func():
            # ignore ctrl-c / SIGINT
            signal.signal(signal.SIGINT, signal.SIG_IGN)
        preexec_fn = preexec_func
    return Popen(list(filter(lambda c: len(c) != 0, command)),
                 stdin=PIPE, stderr=PIPE, preexec_fn=preexec_fn, env=env)


if __name__ == "__main__":
    launch_gateway_server_process(os.environ, sys.argv[1:])
