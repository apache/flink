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
#  limitations under the License.
################################################################################
from typing import List, Dict, Optional

from pyflink.java_gateway import get_gateway
from pyflink.table.catalog import ResolvedSchema
from pyflink.table.table import Table


class Model(object):
    """
    A model object represents a machine learning model registered in the catalog.
    It provides a way to perform predictions on input tables using the predict method.
    """

    def __init__(self, j_model):
        self._j_model = j_model

    def predict(self,
                input_table: Table,
                input_columns: List[str],
                options: Optional[Dict[str, str]] = None) -> Table:
        """
        Performs prediction on the given input table using this model.

        Example:
        ::

            >>> model = t_env.from_model("my_model")
            >>> input_table = t_env.from_path("src")
            >>> result_table = model.predict(input_table, ["user_comment"])
            >>> result_table.execute_insert("prediction_result").wait()

        :param input_table: The input table to perform prediction on.
        :param input_columns: The column names from the input table to use as features.
        :param options: Optional runtime configuration for the prediction.
        :return: The resulting table with prediction results appended.
        """
        gateway = get_gateway()

        # Convert input columns to Java ColumnList
        j_input_columns = gateway.jvm.java.util.ArrayList()
        for col_item in input_columns:
            if isinstance(col_item, str):
                j_input_columns.add(col_item)
            else:
                j_input_columns.add(str(col_item))
        j_column_list = gateway.jvm.org.apache.flink.types.ColumnList.of(j_input_columns)

        # Convert config to Java map if provided
        if options is not None:
            j_config = gateway.jvm.java.util.HashMap()
            for key, value in options.items():
                j_config.put(str(key), str(value))
        else:
            j_config = gateway.jvm.java.util.Collections.emptyMap()

        j_result_table = self._j_model.predict(input_table._j_table, j_column_list, j_config)
        return Table(j_result_table, input_table._t_env)

    def get_resolved_input_schema(self) -> 'ResolvedSchema':
        """
        Returns the resolved input schema of this model.

        The input schema describes the structure and data types of the input columns that the
        model expects for inference operations.

        :return: the resolved input schema.
        """
        j_resolved_schema = self._j_model.getResolvedInputSchema()
        return ResolvedSchema(j_resolved_schema=j_resolved_schema)

    def get_resolved_output_schema(self) -> 'ResolvedSchema':
        """
        Returns the resolved output schema of this model.

        The output schema describes the structure and data types of the output columns that the
        model produces during inference operations.

        :return: the resolved output schema.
        """
        j_resolved_schema = self._j_model.getResolvedOutputSchema()
        return ResolvedSchema(j_resolved_schema=j_resolved_schema)

    @property
    def j_model(self):
        return self._j_model
