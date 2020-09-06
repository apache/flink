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

__all__ = ['SqlDialect']


class SqlDialect(object):
    """
    Enumeration of valid SQL compatibility modes.

    In most of the cases, the built-in compatibility mode should be sufficient. For some features,
    i.e. the "INSERT INTO T PARTITION(a='xxx') ..." grammar, you may need to switch to the
    Hive dialect if required.

    We may introduce other SQL dialects in the future.

    :data:`DEFAULT`:

    Flink's default SQL behavior.

    :data:`HIVE`:

    SQL dialect that allows some Apache Hive specific grammar.

    Note: We might never support all of the Hive grammar. See the documentation for
    supported features.
    """

    DEFAULT = 0
    HIVE = 1

    @staticmethod
    def _from_j_sql_dialect(j_sql_dialect):
        gateway = get_gateway()
        JSqlDialect = gateway.jvm.org.apache.flink.table.api.SqlDialect
        if j_sql_dialect == JSqlDialect.DEFAULT:
            return SqlDialect.DEFAULT
        elif j_sql_dialect == JSqlDialect.HIVE:
            return SqlDialect.HIVE
        else:
            raise Exception("Unsupported Java SQL dialect: %s" % j_sql_dialect)

    @staticmethod
    def _to_j_sql_dialect(sql_dialect):
        gateway = get_gateway()
        JSqlDialect = gateway.jvm.org.apache.flink.table.api.SqlDialect
        if sql_dialect == SqlDialect.DEFAULT:
            return JSqlDialect.DEFAULT
        elif sql_dialect == SqlDialect.HIVE:
            return JSqlDialect.HIVE
        else:
            raise TypeError("Unsupported SQL dialect: %s, supported SQL dialects are: "
                            "SqlDialect.DEFAULT, SqlDialect.HIVE." % sql_dialect)
