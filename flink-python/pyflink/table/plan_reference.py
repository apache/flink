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
from typing import Union

from pyflink.java_gateway import get_gateway
from pathlib import Path

__all__ = [
    "PlanReference",
    "FilePlanReference",
    "JsonContentPlanReference",
    "BytesContentPlanReference",
]


class PlanReference(object):
    """
    Unresolved pointer to a persisted plan.

    A plan represents a static, executable entity that has been compiled from a Table & SQL API
    pipeline definition.

    You can load the content of this reference into a :class:`~pyflink.table.CompiledPlan`
    using :func:`~pyflink.table.TableEnvironment.load_plan` with a
    :class:`~pyflink.table.PlanReference`, or you can directly load and execute it with
    :func:`~pyflink.table.TableEnvironment.execute_plan`.

    .. seealso:: :class:`~pyflink.table.CompiledPlan`
    """

    def __init__(self, j_plan_reference):
        self._j_plan_reference = j_plan_reference

    def __str__(self):
        return self._j_plan_reference.toString()

    def __hash__(self):
        return self._j_plan_reference.hashCode()

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self._j_plan_reference.equals(
            other._j_plan_reference
        )

    @staticmethod
    def from_file(path: Union[str, Path]) -> "FilePlanReference":
        """
        Create a reference starting from a file path.
        """
        gateway = get_gateway()
        return FilePlanReference(
            j_plan_reference=gateway.jvm.org.apache.flink.table.api.PlanReference.fromFile(
                str(path)
            )
        )

    @staticmethod
    def from_json_string(json_string: str) -> "JsonContentPlanReference":
        """
        Create a reference starting from a JSON string.
        """
        gateway = get_gateway()
        return JsonContentPlanReference(
            j_plan_reference=gateway.jvm.org.apache.flink.table.api.PlanReference.fromJsonString(
                json_string
            )
        )

    @staticmethod
    def from_smile_bytes(smile_bytes: bytes) -> "BytesContentPlanReference":
        """
        Create a reference starting from a Smile binary representation.
        """
        gateway = get_gateway()
        return BytesContentPlanReference(
            j_plan_reference=gateway.jvm.org.apache.flink.table.api.PlanReference.fromSmileBytes(
                smile_bytes
            )
        )


class FilePlanReference(PlanReference):
    """
    Plan reference to a file in the local filesystem.
    """

    def get_file(self) -> Path:
        """
        Get the canonical path of the referenced file.
        """
        return Path(self._j_plan_reference.getFile().getCanonicalPath())


class JsonContentPlanReference(PlanReference):
    """
    Plan reference to a string containing the serialized persisted plan in JSON.
    """

    def get_content(self) -> str:
        """
        Get the content of the referenced plan as a JSON string.
        """
        return self._j_plan_reference.getContent()


class BytesContentPlanReference(PlanReference):
    """
    Plan reference to binary bytes containing the serialized persisted plan in Smile.
    """

    def get_content(self) -> bytes:
        """
        Return the content of the referenced plan as Smile binary bytes.
        """
        return self._j_plan_reference.getContent()
