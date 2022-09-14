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
from pyflink.datastream.slot_sharing_group import MemorySize, SlotSharingGroup
from pyflink.testing.test_case_utils import PyFlinkTestCase


class SlotSharingGroupTests(PyFlinkTestCase):

    def test_build_slot_sharing_group_with_specific_resource(self):
        name = 'slot_sharing_group'
        heap_memory = MemorySize.of_mebi_bytes(100)
        off_heap_memory = MemorySize.of_mebi_bytes(200)
        managed_memory = MemorySize.of_mebi_bytes(300)
        slot_sharing_group = SlotSharingGroup.builder(name) \
            .set_cpu_cores(1.0) \
            .set_task_heap_memory(heap_memory) \
            .set_task_off_heap_memory(off_heap_memory) \
            .set_managed_memory(managed_memory) \
            .set_external_resource('gpu', 1.0) \
            .build()
        self.assertEqual(slot_sharing_group.get_name(), name)
        self.assertEqual(slot_sharing_group.get_cpu_cores(), 1.0)
        self.assertEqual(slot_sharing_group.get_task_heap_memory(), heap_memory)
        self.assertEqual(slot_sharing_group.get_task_off_heap_memory(), off_heap_memory)
        self.assertEqual(slot_sharing_group.get_managed_memory(), managed_memory)
        self.assertEqual(slot_sharing_group.get_external_resources(), {'gpu': 1.0})

    def test_build_slot_sharing_group_with_unknown_resource(self):
        name = 'slot_sharing_group'
        slot_sharing_group = SlotSharingGroup.builder(name).build()
        self.assertEqual(slot_sharing_group.get_name(), name)
        self.assertIsNone(slot_sharing_group.get_cpu_cores())
        self.assertIsNone(slot_sharing_group.get_task_heap_memory())
        self.assertIsNone(slot_sharing_group.get_task_off_heap_memory())
        self.assertIsNone(slot_sharing_group.get_managed_memory())
        self.assertEqual(slot_sharing_group.get_external_resources(), {})

    def test_build_slot_sharing_group_with_illegal_config(self):
        with self.assertRaises(Exception):
            SlotSharingGroup.builder("slot_sharing_group") \
                .set_cpu_cores(1.0) \
                .set_task_heap_memory(MemorySize(bytes_size=0)) \
                .set_task_off_heap_memory_mb(10) \
                .build()

    def test_build_slot_sharing_group_without_all_required_config(self):
        with self.assertRaises(Exception):
            SlotSharingGroup.builder("slot_sharing_group") \
                .set_cpu_cores(1.0) \
                .set_task_off_heap_memory_mb(10) \
                .build()
