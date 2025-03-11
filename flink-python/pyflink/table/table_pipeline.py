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

from typing import Optional
from pyflink.java_gateway import get_gateway
from pyflink.table import ExplainDetail
from pyflink.table.catalog import ObjectIdentifier
from pyflink.table.compiled_plan import CompiledPlan
from pyflink.table.table_result import TableResult
from pyflink.util.java_utils import to_j_explain_detail_arr

__all__ = ["TablePipeline"]


class TablePipeline(object):
    """
    Describes a complete pipeline from one or more source tables to a sink table.
    """

    def __init__(self, j_table_pipeline, t_env):
        self._j_table_pipeline = j_table_pipeline
        self._t_env = t_env

    def __str__(self) -> str:
        return self._j_table_pipeline.toString()

    def execute(self) -> TableResult:
        """
        Executes the table pipeline.

        .. versionadded:: 2.1.0
        """
        self._t_env._before_execute()
        return TableResult(self._j_table_pipeline.execute())

    def explain(self, *extra_details: ExplainDetail) -> str:
        """
        Returns the AST and the execution plan of the table pipeline.

        :param extra_details: The extra explain details which the explain result should include,
                              e.g. estimated cost, changelog mode for streaming
        :return: AST and execution plans

        .. versionadded:: 2.1.0
        """
        gateway = get_gateway()
        j_extra_details = to_j_explain_detail_arr(extra_details)
        return self._j_table_pipeline.explain(
            gateway.jvm.org.apache.flink.table.api.ExplainFormat.TEXT, j_extra_details
        )

    def print_explain(self, *extra_details: ExplainDetail):
        """
        Like :func:`~pyflink.table.TablePipeline.explain`, but prints the result to the client
        console.

        .. versionadded:: 2.1.0
        """
        print(self.explain(*extra_details))

    def get_sink_identifier(self) -> Optional[ObjectIdentifier]:
        """
        Returns the sink table's :class:`~pyflink.table.catalog.ObjectIdentifier`, if any.
        The result is empty for anonymous sink tables that haven't been registered before.
        Registering sink tables can be done via
        :func:`~pyflink.table.TableEnvironment.create_temporary_table` using a
        :class:`~pyflink.table.TableDescriptor`.

        .. versionadded:: 2.1.0
        """
        optional_result = self._j_table_pipeline.getSinkIdentifier()
        return (
            ObjectIdentifier(j_object_identifier=optional_result.get())
            if optional_result.isPresent()
            else None
        )

    def compile_plan(self) -> CompiledPlan:
        """
        Compiles this :class:`TablePipeline` into a :class:`~pyflink.table.CompiledPlan` that can
        be executed as one job.

        :class:`~pyflink.table.CompiledPlan`s can be persisted and reloaded across Flink versions.
        They describe static pipelines to ensure backwards compatibility and enable stateful
        streaming job upgrades. See :class:`~pyflink.table.CompiledPlan` and the website
        documentation for more information.

        .. note::
            The compiled plan feature is experimental in batch mode.

        :raises TableException: if any of the statements is invalid or if the plan cannot be
            persisted.

        .. versionadded:: 2.1.0
        """
        return CompiledPlan(j_compiled_plan=self._j_table_pipeline.compilePlan(), t_env=self._t_env)
