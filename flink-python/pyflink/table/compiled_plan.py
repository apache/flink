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
from pathlib import Path
from typing import Union

from pyflink.java_gateway import get_gateway
from pyflink.table.explain_detail import ExplainDetail
from pyflink.table.table_result import TableResult
from pyflink.util.api_stability_decorators import Experimental
from pyflink.util.java_utils import to_j_explain_detail_arr

__all__ = ["CompiledPlan"]


@Experimental()
class CompiledPlan(object):
    """
    Represents an immutable, fully optimized, and executable entity that has been compiled from a
    Table & SQL API pipeline definition. It encodes operators, expressions, functions, data types,
    and table connectors.

    Every new Flink version might introduce improved optimizer rules, more efficient operators,
    and other changes that impact the behavior of previously defined pipelines. In order to ensure
    backwards compatibility and enable stateful streaming job upgrades, compiled plans can be
    persisted and reloaded across Flink versions. See the website documentation for more
    information about provided guarantees during stateful pipeline upgrades.

    A plan can be compiled from a SQL query using
    :func:`~pyflink.table.TableEnvironment.compile_plan_sql`.
    It can be persisted using :func:`~pyflink.table.CompiledPlan.write_to_file` or by manually
    extracting the JSON representation with func:`~pyflink.table.CompiledPlan.as_json_string`.
    A plan can be loaded back from a file or a string using
    :func:`~pyflink.table.TableEnvironment.load_plan` with a :class:`~pyflink.table.PlanReference`.
    Instances can be executed using :func:`~pyflink.table.CompiledPlan.execute`.

    Depending on the configuration, permanent catalog metadata (such as information about tables
    and functions) will be persisted in the plan as well. Anonymous/inline objects will be
    persisted (including schema and options) if possible or fail the compilation otherwise.
    For temporary objects, only the identifier is part of the plan and the object needs to be
    present in the session context during a restore.

    JSON encoding is assumed to be the default representation of a compiled plan in all API
    endpoints, and is the format used to persist the plan to files by default.
    For advanced use cases, :func:`~pyflink.table.CompiledPlan.as_smile_bytes` provides a binary
    format representation of the compiled plan.

    .. note::
        Plan restores assume a stable session context. Configuration, loaded modules and
        catalogs, and temporary objects must not change. Schema evolution and changes of function
        signatures are not supported.
    """

    def __init__(self, j_compiled_plan, t_env):
        self._j_compiled_plan = j_compiled_plan
        self._t_env = t_env

    def __str__(self) -> str:
        return self._j_compiled_plan.toString()

    def as_json_string(self) -> str:
        """
        Convert the plan to a JSON string representation.
        """
        return self._j_compiled_plan.asJsonString()

    def as_smile_bytes(self) -> bytes:
        """
        Convert the plan to a Smile binary representation.
        """
        return self._j_compiled_plan.asSmileBytes()

    def write_to_file(self, file: Union[str, Path], ignore_if_exists: bool = False):
        """
        Writes this plan to a file using the JSON representation.

        :param ignore_if_exists: If a plan exists in the given file path and this flag is true,
            no operation is executed and the plan is not overwritten. An exception is thrown
            otherwise.
        :raises TableException: if the file cannot be written or if ``ignore_if_exists`` is false
            and a plan already exists.
        """
        self._j_compiled_plan.writeToFile(str(file), ignore_if_exists)

    def get_flink_version(self) -> str:
        """
        Returns the Flink version used to compile the plan.
        """
        return str(self._j_compiled_plan.getFlinkVersion())

    def print_json_string(self):
        """
        Like :func:`~pyflink.table.CompiledPlan.as_json_string`, but prints the result to the
        client console.

        .. versionadded:: 2.1.0
        """
        self._j_compiled_plan.printJsonString()

    def execute(self) -> TableResult:
        """
        Executes the compiled plan.
        """
        self._t_env._before_execute()
        return TableResult(self._j_compiled_plan.execute())

    def explain(self, *extra_details: ExplainDetail) -> str:
        """
        Returns the AST and the execution plan of the compiled plan.

        :param extra_details: The extra explain details which the explain result should include,
                              e.g. estimated cost, changelog mode for streaming
        :return: AST and execution plans
        """
        gateway = get_gateway()
        j_extra_details = to_j_explain_detail_arr(extra_details)
        return self._j_compiled_plan.explain(
            gateway.jvm.org.apache.flink.table.api.ExplainFormat.TEXT, j_extra_details
        )

    def print_explain(self, *extra_details: ExplainDetail):
        """
        Like :func:`~pyflink.table.CompiledPlan.explain`, but prints the result to the client
        console.

        .. versionadded:: 2.1.0
        """
        print(self.explain(*extra_details))
