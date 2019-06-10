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
from pyflink.common import (MemoryStateBackend, FsStateBackend, RocksDBStateBackend,
                            PredefinedOptions)
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import PyFlinkTestCase


class MemoryStateBackendTests(PyFlinkTestCase):

    def test_constant(self):
        gateway = get_gateway()
        JMemoryStateBackend = gateway.jvm.org.apache.flink.runtime.state.memory \
            .MemoryStateBackend

        assert MemoryStateBackend.DEFAULT_MAX_STATE_SIZE == \
            JMemoryStateBackend.DEFAULT_MAX_STATE_SIZE

    def test_create_memory_state_backend(self):

        MemoryStateBackend("file://var/checkpoints/")

        MemoryStateBackend("file://var/checkpoints/", "file://var/savepoints/")

        MemoryStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 10000000)

        MemoryStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 10000000, True)

        MemoryStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 10000000, False)

    def test_configure(self):

        state_backend = MemoryStateBackend()

        assert state_backend.is_using_asynchronous_snapshots() is True

        state_backend = state_backend.configure({"state.backend.async": "False"})

        assert state_backend.is_using_asynchronous_snapshots() is False

    def test_is_using_asynchronous_snapshots(self):

        state_backend = MemoryStateBackend()

        assert state_backend.is_using_asynchronous_snapshots() is True

        state_backend = MemoryStateBackend(using_asynchronous_snapshots=True)

        assert state_backend.is_using_asynchronous_snapshots() is True

        state_backend = MemoryStateBackend(using_asynchronous_snapshots=False)

        assert state_backend.is_using_asynchronous_snapshots() is False

    def test_get_max_state_size(self):

        state_backend = MemoryStateBackend()

        assert state_backend.get_max_state_size() == MemoryStateBackend.DEFAULT_MAX_STATE_SIZE

        state_backend = MemoryStateBackend(max_state_size=50000)

        assert state_backend.get_max_state_size() == 50000


class FsStateBackendTests(PyFlinkTestCase):

    def test_create_fs_state_backend(self):

        FsStateBackend("file://var/checkpoints/")

        FsStateBackend("file://var/checkpoints/", "file://var/savepoints/")

        FsStateBackend("file://var/checkpoints/", "file://var/savepoints/", 2048)

        FsStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 2048, True)

        FsStateBackend(
            "file://var/checkpoints/", "file://var/savepoints/", 2048, False)

    def test_configure(self):

        state_backend = FsStateBackend("file://var/checkpoints/")

        assert state_backend.is_using_asynchronous_snapshots() is True

        state_backend = state_backend.configure({"state.backend.async": "False"})

        assert state_backend.is_using_asynchronous_snapshots() is False

    def test_get_min_file_size_threshold(self):

        state_backend = FsStateBackend("file://var/checkpoints/")

        assert state_backend.get_min_file_size_threshold() == 1024

        state_backend = FsStateBackend("file://var/checkpoints/", file_state_size_threshold=2048)

        assert state_backend.get_min_file_size_threshold() == 2048

    def test_get_checkpoint_path(self):

        state_backend = FsStateBackend("file://var/checkpoints/")

        assert state_backend.get_checkpoint_path() == "file://var/checkpoints"


class RocksDBStateBackendTests(PyFlinkTestCase):

    def test_create_rocks_db_state_backend(self):

        RocksDBStateBackend("file://var/checkpoints/")

        RocksDBStateBackend("file://var/checkpoints/", True)

        RocksDBStateBackend("file://var/checkpoints/", False)

        RocksDBStateBackend(
            checkpoint_stream_backend=FsStateBackend("file://var/checkpoints/"))

    def test_configure(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        assert state_backend.is_incremental_checkpoints_enabled() is False

        state_backend = state_backend.configure({"state.backend.incremental": "True"})

        assert state_backend.is_incremental_checkpoints_enabled() is True

    def test_get_checkpoint_backend(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        checkpoint_backend = state_backend.get_checkpoint_backend()

        assert isinstance(checkpoint_backend, FsStateBackend)
        assert checkpoint_backend.get_checkpoint_path() == "file://var/checkpoints"

    def test_get_set_db_storage_paths(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        state_backend.set_db_storage_paths("file://var/db_storage_dir1/",
                                           "file://var/db_storage_dir2/",
                                           "file://var/db_storage_dir3/")

        assert state_backend.get_db_storage_paths() == ['/db_storage_dir1',
                                                        '/db_storage_dir2',
                                                        '/db_storage_dir3']

    def test_get_set_ttl_compaction_filter(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        assert state_backend.is_ttl_compaction_filter_enabled() is False

        state_backend.enable_ttl_compaction_filter()

        assert state_backend.is_ttl_compaction_filter_enabled() is True

    def test_get_set_predefined_options(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        assert state_backend.get_predefined_options() == PredefinedOptions.DEFAULT

        state_backend.set_predefined_options(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)

        assert state_backend.get_predefined_options() == \
            PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM

        state_backend.set_predefined_options(PredefinedOptions.SPINNING_DISK_OPTIMIZED)

        assert state_backend.get_predefined_options() == PredefinedOptions.SPINNING_DISK_OPTIMIZED

        state_backend.set_predefined_options(PredefinedOptions.FLASH_SSD_OPTIMIZED)

        assert state_backend.get_predefined_options() == PredefinedOptions.FLASH_SSD_OPTIMIZED

        state_backend.set_predefined_options(PredefinedOptions.DEFAULT)

        assert state_backend.get_predefined_options() == PredefinedOptions.DEFAULT

    def test_get_set_options(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        assert state_backend.get_options() is None

        state_backend.set_options(
            "org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory")

        assert state_backend.get_options() == \
            "org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory"

    def test_get_set_number_of_transfering_threads(self):

        state_backend = RocksDBStateBackend("file://var/checkpoints/")

        assert state_backend.get_number_of_transfering_threads() == 1

        state_backend.set_number_of_transfering_threads(4)

        assert state_backend.get_number_of_transfering_threads() == 4
