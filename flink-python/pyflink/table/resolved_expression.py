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
from typing import List

from pyflink.table import Expression
from pyflink.table.types import DataType, _from_java_data_type

__all__ = ["ResolvedExpression"]


class ResolvedExpression(Expression):
    """
    Expression that has been fully resolved and validated.

    Compared to :class:`~pyflink.table.Expression`, resolved expressions do not contain unresolved
    subexpressions anymore and provide an output data type for the computation result.

    Instances of this class describe a fully parameterized, immutable expression that can be
    serialized and persisted.

    Resolved expression are the output of the API to the planner and are pushed from the planner
    into interfaces, for example, for predicate push-down.
    """

    def __init__(self, j_resolved_expr):
        super().__init__(j_expr_or_property_name=j_resolved_expr)
        self._j_resolved_expr = j_resolved_expr

    def get_output_data_type(self) -> DataType:
        """
        Returns the data type of the computation result.
        """
        j_data_type = self._j_resolved_expr.getOutputDataType()
        return _from_java_data_type(j_data_type)

    def get_resolved_children(self) -> List["ResolvedExpression"]:
        j_resolved_children = self._j_resolved_expr.getResolvedChildren()
        return [ResolvedExpression(j_resolved_child) for j_resolved_child in j_resolved_children]
