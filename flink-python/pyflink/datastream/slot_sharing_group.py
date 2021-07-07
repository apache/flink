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

__all__ = ['MemorySize', 'SlotSharingGroup']

from typing import Optional

from pyflink.java_gateway import get_gateway


class MemorySize(object):
    """
    MemorySize is a representation of a number of bytes, viewable in different units.
    """

    def __init__(self, j_memory_size=None, bytes_size: int = None):
        self._j_memory_size = get_gateway().jvm \
            .org.apache.flink.configuration.MemorySize(bytes_size) \
            if j_memory_size is None else j_memory_size

    @staticmethod
    def of_mebi_bytes(mebi_bytes: int) -> 'MemorySize':
        return MemorySize(
            get_gateway().jvm.org.apache.flink.configuration.MemorySize.ofMebiBytes(mebi_bytes))

    def get_bytes(self) -> int:
        """
        Gets the memory size in bytes.

        :return: The memory size in bytes.
        """
        return self._j_memory_size.getBytes()

    def get_kibi_bytes(self) -> int:
        """
        Gets the memory size in Kibibytes (= 1024 bytes).

        :return: The memory size in Kibibytes.
        """
        return self._j_memory_size.getKibiBytes()

    def get_mebi_bytes(self) -> int:
        """
        Gets the memory size in Mebibytes (= 1024 Kibibytes).

        :return: The memory size in Mebibytes.
        """
        return self._j_memory_size.getMebiBytes()

    def get_gibi_bytes(self) -> int:
        """
        Gets the memory size in Gibibytes (= 1024 Mebibytes).

        :return: The memory size in Gibibytes.
        """
        return self._j_memory_size.getGibiBytes()

    def get_tebi_bytes(self) -> int:
        """
        Gets the memory size in Tebibytes (= 1024 Gibibytes).

        :return: The memory size in Tebibytes.
        """
        return self._j_memory_size.getTebiBytes()

    def get_java_memory_size(self):
        """
        Gets the Java MemorySize object.

        :return: The Java MemorySize object.
        """
        return self._j_memory_size

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._j_memory_size == other._j_memory_size

    def __hash__(self):
        return self._j_memory_size.hashCode()

    def __lt__(self, other: 'MemorySize'):
        if not isinstance(other, MemorySize):
            raise Exception("Does not support comparison with non-MemorySize %s" % other)

        return self._j_memory_size.compareTo(other._j_memory_size) == -1

    def __le__(self, other: 'MemorySize'):
        return self.__eq__(other) and self.__lt__(other)

    def __str__(self):
        return self._j_memory_size.toString()


class SlotSharingGroup(object):
    """
    Describe the name and the the different resource components of a slot sharing group.
    """

    def __init__(self, j_slot_sharing_group):
        self._j_slot_sharing_group = j_slot_sharing_group

    def get_name(self) -> str:
        """
        Gets the name of this SlotSharingGroup.

        :return: The name of the SlotSharingGroup.
        """
        return self._j_slot_sharing_group.getName()

    def get_managed_memory(self) -> Optional[MemorySize]:
        """
        Gets the task managed memory for this SlotSharingGroup.

        :return: The task managed memory of the SlotSharingGroup.
        """
        managed_memory = self._j_slot_sharing_group.getManagedMemory()
        return MemorySize(managed_memory.get()) if managed_memory.isPresent() else None

    def get_task_heap_memory(self) -> Optional[MemorySize]:
        """
        Gets the task heap memory for this SlotSharingGroup.

        :return: The task heap memory of the SlotSharingGroup.
        """
        task_heap_memory = self._j_slot_sharing_group.getTaskHeapMemory()
        return MemorySize(task_heap_memory.get()) if task_heap_memory.isPresent() else None

    def get_task_off_heap_memory(self) -> Optional[MemorySize]:
        """
        Gets the task off-heap memory for this SlotSharingGroup.

        :return: The task off-heap memory of the SlotSharingGroup.
        """
        task_off_heap_memory = self._j_slot_sharing_group.getTaskOffHeapMemory()
        return MemorySize(task_off_heap_memory.get()) if task_off_heap_memory.isPresent() else None

    def get_cpu_cores(self) -> Optional[float]:
        """
       Gets the CPU cores for this SlotSharingGroup.

        :return: The CPU cores of the SlotSharingGroup.
        """
        cpu_cores = self._j_slot_sharing_group.getCpuCores()
        return cpu_cores.get() if cpu_cores.isPresent() else None

    def get_external_resources(self) -> dict:
        """
        Gets the external resource from this SlotSharingGroup.

        :return: User specified resources of the SlotSharingGroup.
        """
        return dict(self._j_slot_sharing_group.getExternalResources())

    def get_java_slot_sharing_group(self):
        """
        Gets the Java SlotSharingGroup object.

        :return: The Java SlotSharingGroup object.
        """
        return self._j_slot_sharing_group

    @staticmethod
    def builder(name: str) -> 'Builder':
        """
        Gets the Builder with the given name for this SlotSharingGroup.

        :param name: The name of the SlotSharingGroup.
        :return: The builder for the SlotSharingGroup.
        """
        return SlotSharingGroup.Builder(
            get_gateway().jvm.org.apache.flink.api.common.operators.SlotSharingGroup.newBuilder(
                name))

    def __eq__(self, other):
        return isinstance(other, self.__class__) and \
            self._j_slot_sharing_group == other._j_slot_sharing_group

    def __hash__(self):
        return self._j_slot_sharing_group.hashCode()

    class Builder(object):
        """
        Builder for the SlotSharingGroup.
        """

        def __init__(self, j_builder):
            self._j_builder = j_builder

        def set_cpu_cores(self, cpu_cores: float) -> 'SlotSharingGroup.Builder':
            """
            Sets the CPU cores for this SlotSharingGroup.

            :param cpu_cores: The CPU cores of the SlotSharingGroup.
            :return: This object.
            """
            self._j_builder.setCpuCores(cpu_cores)
            return self

        def set_task_heap_memory(self, task_heap_memory: MemorySize) -> 'SlotSharingGroup.Builder':
            """
            Sets the task heap memory for this SlotSharingGroup.

            :param task_heap_memory: The task heap memory of the SlotSharingGroup.
            :return: This object.
            """
            self._j_builder.setTaskHeapMemory(task_heap_memory.get_java_memory_size())
            return self

        def set_task_heap_memory_mb(self, task_heap_memory_mb: int) -> 'SlotSharingGroup.Builder':
            """
            Sets the task heap memory for this SlotSharingGroup in MB.

            :param task_heap_memory_mb: The task heap memory of the SlotSharingGroup in MB.
            :return: This object.
            """
            self._j_builder.setTaskHeapMemoryMB(task_heap_memory_mb)
            return self

        def set_task_off_heap_memory(self, task_off_heap_memory: MemorySize) \
                -> 'SlotSharingGroup.Builder':
            """
            Sets the task off-heap memory for this SlotSharingGroup.

            :param task_off_heap_memory: The task off-heap memory of the SlotSharingGroup.
            :return: This object.
            """
            self._j_builder.setTaskOffHeapMemory(task_off_heap_memory.get_java_memory_size())
            return self

        def set_task_off_heap_memory_mb(self, task_off_heap_memory_mb: int) \
                -> 'SlotSharingGroup.Builder':
            """
            Sets the task off-heap memory for this SlotSharingGroup in MB.

            :param task_off_heap_memory_mb: The task off-heap memory of the SlotSharingGroup in MB.
            :return: This object.
            """
            self._j_builder.setTaskOffHeapMemoryMB(task_off_heap_memory_mb)
            return self

        def set_managed_memory(self, managed_memory: MemorySize) -> 'SlotSharingGroup.Builder':
            """
            Sets the task managed memory for this SlotSharingGroup.

            :param managed_memory: The task managed memory of the SlotSharingGroup.
            :return: This object.
            """
            self._j_builder.setManagedMemory(managed_memory.get_java_memory_size())
            return self

        def set_managed_memory_mb(self, managed_memory_mb: int) -> 'SlotSharingGroup.Builder':
            """
            Sets the task managed memory for this SlotSharingGroup in MB.

            :param managed_memory_mb: The task managed memory of the SlotSharingGroup in MB.
            :return: This object.
            """
            self._j_builder.setManagedMemoryMB(managed_memory_mb)
            return self

        def set_external_resource(self, name: str, value: float) -> 'SlotSharingGroup.Builder':
            """
            Adds the given external resource. The old value with the same resource name will be
            replaced if present.

            :param name: The resource name of the given external resource.
            :param value: The value of the given external resource.
            :return: This object.
            """
            self._j_builder.setExternalResource(name, value)
            return self

        def build(self) -> 'SlotSharingGroup':
            """
            Builds the SlotSharingGroup.

            :return: The SlotSharingGroup object.
            """
            return SlotSharingGroup(j_slot_sharing_group=self._j_builder.build())
