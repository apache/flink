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
                                              MemoryStateBackend, FsStateBackend,
                                              RocksDBStateBackend, PredefinedOptions)
from pyflink.java_gateway import get_gateway
from pyflink.pyflink_gateway_server import on_windows
from pyflink.testing.test_case_utils import PyFlinkTestCase
from pyflink.util.utils import load_java_class


class MemoryStateBackendTests(PyFlinkTestCase):

    def test_constant(self):
        gateway = get_gateway()
        JMemoryStateBackend = gateway.jvm.org.apache.flink.runtime.state.memory \
            .MemoryStateBackend

        self.assertEqual(MemoryStateBackend.DEFAULT_MAX_STATE_SIZE,
                         JMemoryStateBackend.DEFAULT_MAX_STATE_SIZE)

    def test_create_memory_state_backend(self):

        self.assertIsNotNone(MemoryStateBackend("file://var/checkpoints/"))

        self.assertIsNotNone(MemoryStateBackend("file://var/checkpoints/",
                                                "file://var/savepoints/"))

        self.assertIsNotNone(MemoryStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 10000000))

        self.assertIsNotNone(MemoryStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 10000000, True))

        self.assertIsNotNone(MemoryStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 10000000, False))

    def test_is_using_asynchronous_snapshots(self):

        state_backend = MemoryStateBackend()

        self.assertTrue(state_backend.is_using_asynchronous_snapshots())

        state_backend = MemoryStateBackend(using_asynchronous_snapshots=True)

        self.assertTrue(state_backend.is_using_asynchronous_snapshots())

        state_backend = MemoryStateBackend(using_asynchronous_snapshots=False)

        self.assertFalse(state_backend.is_using_asynchronous_snapshots())

    def test_get_max_state_size(self):

        state_backend = MemoryStateBackend()

        self.assertEqual(state_backend.get_max_state_size(),
                         MemoryStateBackend.DEFAULT_MAX_STATE_SIZE)

        state_backend = MemoryStateBackend(max_state_size=50000)

        self.assertEqual(state_backend.get_max_state_size(), 50000)


class FsStateBackendTests(PyFlinkTestCase):

    def test_create_fs_state_backend(self):

        self.assertIsNotNone(FsStateBackend("file://var/checkpoints/"))

        self.assertIsNotNone(FsStateBackend("file://var/checkpoints/", "file://var/savepoints/"))

        self.assertIsNotNone(FsStateBackend("file://var/checkpoints/",
                                            "file://var/savepoints/", 2048))

        self.assertIsNotNone(FsStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 2048, 2048, True))

        self.assertIsNotNone(FsStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 2048, 4096))

    def test_get_min_file_size_threshold(self):

        state_backend = FsStateBackend("file://var/checkpoints/")

        self.assertEqual(state_backend.get_min_file_size_threshold(), 20480)

        state_backend = FsStateBackend("file://var/checkpoints/", file_state_size_threshold=2048)

        self.assertEqual(state_backend.get_min_file_size_threshold(), 2048)

    def test_get_checkpoint_path(self):

        state_backend = FsStateBackend("file://var/checkpoints/")

        self.assertEqual(state_backend.get_checkpoint_path(), "file://var/checkpoints")


class RocksDBStateBackendTests(PyFlinkTestCase):

    def test_create_rocks_db_state_backend(self):

        self.assertIsNotNone(RocksDBStateBackend("file://var/checkpoints/"))

        self.assertIsNotNone(RocksDBStateBackend("file://var/checkpoints/", True))

        self.assertIsNotNone(RocksDBStateBackend("file://var/checkpoints/", False))

        self.assertIsNotNone(RocksDBStateBackend(
            checkpoint_stream_backend=FsStateBackend("file://var/checkpoints/")))

    def test_get_checkpoint_backend(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        checkpoint_backend = state_backend.get_checkpoint_backend()

        self.assertIsInstance(checkpoint_backend, FsStateBackend)
        self.assertEqual(checkpoint_backend.get_checkpoint_path(), "file://var/checkpoints")

    def test_get_set_db_storage_paths(self):
        if on_windows():
            checkpoints_path = "file:/C:/var/checkpoints/"
            storage_path = ["file:/C:/var/db_storage_dir1/",
                            "file:/C:/var/db_storage_dir2/",
                            "file:/C:/var/db_storage_dir3/"]
            expected = ["C:\\var\\db_storage_dir1",
                        "C:\\var\\db_storage_dir2",
                        "C:\\var\\db_storage_dir3"]
        else:
            checkpoints_path = "file://var/checkpoints/"
            storage_path = ["file://var/db_storage_dir1/",
                            "file://var/db_storage_dir2/",
                            "file://var/db_storage_dir3/"]
            expected = ["/db_storage_dir1",
                        "/db_storage_dir2",
                        "/db_storage_dir3"]

        state_backend = RocksDBStateBackend(checkpoints_path)
        state_backend.set_db_storage_paths(*storage_path)
        self.assertEqual(state_backend.get_db_storage_paths(), expected)

    def test_get_set_predefined_options(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

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

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        self.assertIsNone(state_backend.get_options())

        state_backend.set_options(
            "org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory")

        self.assertEqual(state_backend.get_options(),
                         "org.apache.flink.contrib.streaming.state."
                         "DefaultConfigurableOptionsFactory")

    def test_get_set_number_of_transfering_threads(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        self.assertEqual(state_backend.get_number_of_transfering_threads(), 1)

        state_backend.set_number_of_transfering_threads(4)

        self.assertEqual(state_backend.get_number_of_transfering_threads(), 4)


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
