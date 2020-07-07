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
from pyflink.java_gateway import get_gateway

__all__ = ['Module', 'HiveModule']


class Module(object):
    """
    Modules define a set of metadata, including functions, user defined types, operators, rules,
    etc.
    Metadata from modules are regarded as built-in or system metadata that users can take advantages
    of.

    .. versionadded:: 1.12.0
    """

    def __init__(self, j_module):
        self._j_module = j_module


class HiveModule(Module):
    """
    Module to provide Hive built-in metadata.

    .. versionadded:: 1.12.0
    """

    def __init__(self, hive_version: str = None):
        gateway = get_gateway()

        if hive_version is None:
            j_hive_module = gateway.jvm.org.apache.flink.table.module.hive.HiveModule()
        else:
            j_hive_module = gateway.jvm.org.apache.flink.table.module.hive.HiveModule(hive_version)
        super(HiveModule, self).__init__(j_hive_module)
