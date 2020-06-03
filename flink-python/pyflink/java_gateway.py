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
import os
import platform
import shlex
import shutil
import signal
import struct
import tempfile
import time
from subprocess import Popen, PIPE
from threading import RLock

from py4j.java_gateway import java_import, JavaGateway, GatewayParameters
from pyflink.find_flink_home import _find_flink_home
from pyflink.util.exceptions import install_exception_handler, install_py4j_hooks

_gateway = None
_lock = RLock()


def is_launch_gateway_disabled():
    if "PYFLINK_GATEWAY_DISABLED" in os.environ \
            and os.environ["PYFLINK_GATEWAY_DISABLED"].lower() not in ["0", "false", ""]:
        return True
    else:
        return False


def get_gateway():
    # type: () -> JavaGateway
    global _gateway
    global _lock
    with _lock:
        if _gateway is None:
            # if Java Gateway is already running
            if 'PYFLINK_GATEWAY_PORT' in os.environ:
                gateway_port = int(os.environ['PYFLINK_GATEWAY_PORT'])
                gateway_param = GatewayParameters(port=gateway_port, auto_convert=True)
                _gateway = JavaGateway(gateway_parameters=gateway_param)
            else:
                _gateway = launch_gateway()

            # import the flink view
            import_flink_view(_gateway)
            install_exception_handler()
            install_py4j_hooks()
    return _gateway


def launch_gateway():
    # type: () -> JavaGateway
    """
    launch jvm gateway
    """
    if is_launch_gateway_disabled():
        raise Exception("It's launching the PythonGatewayServer during Python UDF execution "
                        "which is unexpected. It usually happens when the job codes are "
                        "in the top level of the Python script file and are not enclosed in a "
                        "`if name == 'main'` statement.")
    FLINK_HOME = _find_flink_home()
    # TODO windows support
    on_windows = platform.system() == "Windows"
    if on_windows:
        raise Exception("Windows system is not supported currently.")
    script = "./bin/pyflink-gateway-server.sh"
    command = [os.path.join(FLINK_HOME, script)]
    command += ['-c', 'org.apache.flink.client.python.PythonGatewayServer']

    submit_args = os.environ.get("SUBMIT_ARGS", "local")
    command += shlex.split(submit_args)

    # Create a temporary directory where the gateway server should write the connection information.
    conn_info_dir = tempfile.mkdtemp()
    try:
        fd, conn_info_file = tempfile.mkstemp(dir=conn_info_dir)
        os.close(fd)
        os.unlink(conn_info_file)

        env = dict(os.environ)
        env["_PYFLINK_CONN_INFO_PATH"] = conn_info_file

        def preexec_func():
            # ignore ctrl-c / SIGINT
            signal.signal(signal.SIGINT, signal.SIG_IGN)

        # Launch the Java gateway.
        # We open a pipe to stdin so that the Java gateway can die when the pipe is broken
        p = Popen(command, stdin=PIPE, preexec_fn=preexec_func, env=env)

        while not p.poll() and not os.path.isfile(conn_info_file):
            time.sleep(0.1)

        if not os.path.isfile(conn_info_file):
            raise Exception("Java gateway process exited before sending its port number")

        with open(conn_info_file, "rb") as info:
            gateway_port = struct.unpack("!I", info.read(4))[0]
    finally:
        shutil.rmtree(conn_info_dir)

    # Connect to the gateway
    gateway = JavaGateway(
        gateway_parameters=GatewayParameters(port=gateway_port, auto_convert=True))

    return gateway


def import_flink_view(gateway):
    """
    import the classes used by PyFlink.
    :param gateway:gateway connected to JavaGateWayServer
    """
    # Import the classes used by PyFlink
    java_import(gateway.jvm, "org.apache.flink.table.api.*")
    java_import(gateway.jvm, "org.apache.flink.table.api.java.*")
    java_import(gateway.jvm, "org.apache.flink.table.api.dataview.*")
    java_import(gateway.jvm, "org.apache.flink.table.catalog.*")
    java_import(gateway.jvm, "org.apache.flink.table.descriptors.*")
    java_import(gateway.jvm, "org.apache.flink.table.descriptors.python.*")
    java_import(gateway.jvm, "org.apache.flink.table.sources.*")
    java_import(gateway.jvm, "org.apache.flink.table.sinks.*")
    java_import(gateway.jvm, "org.apache.flink.table.sources.*")
    java_import(gateway.jvm, "org.apache.flink.table.types.*")
    java_import(gateway.jvm, "org.apache.flink.table.types.logical.*")
    java_import(gateway.jvm, "org.apache.flink.table.util.python.*")
    java_import(gateway.jvm, "org.apache.flink.api.common.python.*")
    java_import(gateway.jvm, "org.apache.flink.api.common.typeinfo.TypeInformation")
    java_import(gateway.jvm, "org.apache.flink.api.common.typeinfo.Types")
    java_import(gateway.jvm, "org.apache.flink.api.java.ExecutionEnvironment")
    java_import(gateway.jvm,
                "org.apache.flink.streaming.api.environment.StreamExecutionEnvironment")
    java_import(gateway.jvm, "org.apache.flink.api.common.restartstrategy.RestartStrategies")
