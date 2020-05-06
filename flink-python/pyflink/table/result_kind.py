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

__all__ = ['ResultKind']


class ResultKind(object):
    """
    ResultKind defines the types of the result.

    :data:`SUCCESS`:

    The statement (e.g. DDL, USE) executes successfully, and the result only contains a simple "OK".

    :data:`SUCCESS_WITH_CONTENT`:

    The statement (e.g. DML, DQL, SHOW) executes successfully, and the result contains important
    content.
    """

    SUCCESS = 0
    SUCCESS_WITH_CONTENT = 1

    @staticmethod
    def _from_j_result_kind(j_result_kind):
        gateway = get_gateway()
        JResultKind = gateway.jvm.org.apache.flink.table.api.ResultKind
        if j_result_kind == JResultKind.SUCCESS:
            return ResultKind.SUCCESS
        elif j_result_kind == JResultKind.SUCCESS_WITH_CONTENT:
            return ResultKind.SUCCESS_WITH_CONTENT
        else:
            raise Exception("Unsupported Java result kind: %s" % j_result_kind)
