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
from pyflink.util.api_stability_decorators import PublicEvolving

__all__ = ['ChangelogMode']


@PublicEvolving()
class ChangelogMode(object):
    """
    The set of changes contained in a changelog.
    """

    def __init__(self, j_changelog_mode):
        self._j_changelog_mode = j_changelog_mode

    @staticmethod
    def insert_only():
        """
        Shortcut for a simple :attr:`~pyflink.common.RowKind.INSERT`-only changelog.
        """
        gateway = get_gateway()
        return ChangelogMode(
            gateway.jvm.org.apache.flink.table.connector.ChangelogMode.insertOnly())

    @staticmethod
    def upsert():
        """
        Shortcut for an upsert changelog that describes idempotent updates on a key and thus does
        does not contain :attr:`~pyflink.common.RowKind.UPDATE_BEFORE` rows.
        """
        gateway = get_gateway()
        return ChangelogMode(
            gateway.jvm.org.apache.flink.table.connector.ChangelogMode.upsert())

    @staticmethod
    def all():
        """
        Shortcut for a changelog that can contain all :class:`~pyflink.common.RowKind`.
        """
        gateway = get_gateway()
        return ChangelogMode(
            gateway.jvm.org.apache.flink.table.connector.ChangelogMode.all())
