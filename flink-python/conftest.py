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


def pytest_configure(config):
    # isolate gateway JVM logs per xdist worker
    worker = os.environ.get("PYTEST_XDIST_WORKER")
    if not worker:
        return
    from pyflink.pyflink_gateway_server import get_log_dir, prepare_environment_variables
    env = dict(os.environ)
    prepare_environment_variables(env)
    log_dir = os.path.join(get_log_dir(env), worker)
    os.makedirs(log_dir, exist_ok=True)
    os.environ["FLINK_LOG_DIR"] = log_dir
