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

import unittest

from pyflink.testing.test_case_utils import PythonAPICompletenessTestCase
from pyflink.table.catalog import Catalog, CatalogDatabase, CatalogBaseTable, CatalogPartition, \
    CatalogFunction, CatalogColumnStatistics, CatalogPartitionSpec, ObjectPath


class CatalogAPICompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`Catalog` is consistent with
    Java `org.apache.flink.table.catalog.Catalog`.
    """

    @classmethod
    def python_class(cls):
        return Catalog

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.catalog.Catalog"

    @classmethod
    def excluded_methods(cls):
        # open/close are not needed in Python API as they are used internally
        return {'open', 'close', 'getTableFactory'}


class CatalogDatabaseAPICompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`CatalogDatabase` is consistent with
    Java `org.apache.flink.table.catalog.CatalogDatabase`.
    """

    @classmethod
    def python_class(cls):
        return CatalogDatabase

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.catalog.CatalogDatabase"


class CatalogBaseTableAPICompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`CatalogBaseTable` is consistent with
    Java `org.apache.flink.table.catalog.CatalogBaseTable`.
    """

    @classmethod
    def python_class(cls):
        return CatalogBaseTable

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.catalog.CatalogBaseTable"


class CatalogFunctionAPICompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`CatalogFunction` is consistent with
    Java `org.apache.flink.table.catalog.CatalogFunction`.
    """

    @classmethod
    def python_class(cls):
        return CatalogFunction

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.catalog.CatalogFunction"


class CatalogPartitionAPICompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`CatalogPartition` is consistent with
    Java `org.apache.flink.table.catalog.CatalogPartition`.
    """

    @classmethod
    def python_class(cls):
        return CatalogPartition

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.catalog.CatalogPartition"


class ObjectPathAPICompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`ObjectPath` is consistent with
    Java `org.apache.flink.table.catalog.ObjectPath`.
    """

    @classmethod
    def python_class(cls):
        return ObjectPath

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.catalog.ObjectPath"


class CatalogPartitionSpecAPICompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`CatalogPartitionSpec` is consistent with
    Java `org.apache.flink.table.catalog.CatalogPartitionSpec`.
    """

    @classmethod
    def python_class(cls):
        return CatalogPartitionSpec

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.catalog.CatalogPartitionSpec"


class CatalogColumnStatisticsAPICompletenessTests(PythonAPICompletenessTestCase, unittest.TestCase):
    """
    Tests whether the Python :class:`CatalogColumnStatistics` is consistent with
    Java `org.apache.flink.table.catalog.CatalogColumnStatistics`.
    """

    @classmethod
    def python_class(cls):
        return CatalogColumnStatistics

    @classmethod
    def java_class(cls):
        return "org.apache.flink.table.catalog.stats.CatalogColumnStatistics"


if __name__ == '__main__':
    import unittest

    try:
        import xmlrunner
        testRunner = xmlrunner.XMLTestRunner(output='target/test-reports')
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
