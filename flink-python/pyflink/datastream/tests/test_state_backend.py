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
from pyflink.datastream.state_backend import (_from_j_state_backend, CustomStateBackend,
                                              PredefinedOptions, EmbeddedRocksDBStateBackend)
from pyflink.java_gateway import get_gateway
from pyflink.pyflink_gateway_server import on_windows
from pyflink.testing.test_case_utils import PyFlinkTestCase
from pyflink.util.java_utils import load_java_class


class EmbeddedRocksDBStateBackendTests(PyFlinkTestCase):

    def test_create_rocks_db_state_backend(self):

        self.assertIsNotNone(EmbeddedRocksDBStateBackend())

        self.assertIsNotNone(EmbeddedRocksDBStateBackend(True))

        self.assertIsNotNone(EmbeddedRocksDBStateBackend(False))

    def test_get_set_db_storage_paths(self):
        if on_windows():
            storage_path = ["file:/C:/var/db_storage_dir1/",
                            "file:/C:/var/db_storage_dir2/",
                            "file:/C:/var/db_storage_dir3/"]
            expected = ["C:\\var\\db_storage_dir1",
                        "C:\\var\\db_storage_dir2",
                        "C:\\var\\db_storage_dir3"]
        else:
            storage_path = ["file://var/db_storage_dir1/",
                            "file://var/db_storage_dir2/",
                            "file://var/db_storage_dir3/"]
            expected = ["/db_storage_dir1",
                        "/db_storage_dir2",
                        "/db_storage_dir3"]

        state_backend = EmbeddedRocksDBStateBackend()
        state_backend.set_db_storage_paths(*storage_path)
        self.assertEqual(state_backend.get_db_storage_paths(), expected)

    def test_get_set_predefined_options(self):

        state_backend = EmbeddedRocksDBStateBackend()

        self.assertEqual(state_backend.get_predefined_options(), PredefinedOptions.DEFAULT)

        state_backend.set_predefined_options(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)

        self.assertEqual(state_backend.get_predefined_options(),
                         PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)

        state_backend.set_predefined_options(PredefinedOptions.SPINNING_DISK_OPTIMIZED)

        self.assertEqual(state_backend.get_predefined_options(),
                         PredefinedOptions.SPINNING_DISK_OPTIMIZED)

        state_backend.set_predefined_options(PredefinedOptions.FLASH_SSD_OPTIMIZED)

        self.assertEqual(state_backend.get_predefined_options(),
                         PredefinedOptions.FLASH_SSD_OPTIMIZED)

        state_backend.set_predefined_options(PredefinedOptions.DEFAULT)

        self.assertEqual(state_backend.get_predefined_options(), PredefinedOptions.DEFAULT)

    def test_get_set_options(self):

        state_backend = EmbeddedRocksDBStateBackend()

        self.assertIsNone(state_backend.get_options())

        state_backend.set_options(
            "org.apache.flink.state.rocksdb."
            "RocksDBStateBackendConfigTest$TestOptionsFactory")

        self.assertEqual(state_backend.get_options(),
                         "org.apache.flink.state.rocksdb."
                         "RocksDBStateBackendConfigTest$TestOptionsFactory")

    def test_get_set_number_of_transfer_threads(self):

        state_backend = EmbeddedRocksDBStateBackend()

        self.assertEqual(state_backend.get_number_of_transfer_threads(), 4)

        state_backend.set_number_of_transfer_threads(8)

        self.assertEqual(state_backend.get_number_of_transfer_threads(), 8)


class CustomStateBackendTests(PyFlinkTestCase):

    def test_create_custom_state_backend(self):
        gateway = get_gateway()
        JConfiguration = gateway.jvm.org.apache.flink.configuration.Configuration
        j_config = JConfiguration()
        j_factory = load_java_class("org.apache.flink.streaming.runtime.tasks."
                                    "StreamTaskTest$TestMemoryStateBackendFactory").newInstance()
        context_classloader = gateway.jvm.Thread.currentThread().getContextClassLoader()
        state_backend = _from_j_state_backend(j_factory.createFromConfig(j_config,
                                                                         context_classloader))

        self.assertIsInstance(state_backend, CustomStateBackend)
