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

from pyflink.ml.api.param import WithParams, ParamInfo, TypeConverters


class HasSelectedCols(WithParams):
    """
    An interface for classes with a parameter specifying the name of multiple table columns.
    """

    selected_cols = ParamInfo(
        "selectedCols",
        "Names of the columns used for processing",
        is_optional=False,
        type_converter=TypeConverters.to_list_string)

    def set_selected_cols(self, v: list) -> 'HasSelectedCols':
        return super().set(self.selected_cols, v)

    def get_selected_cols(self) -> list:
        return super().get(self.selected_cols)


class HasOutputCol(WithParams):
    """
    An interface for classes with a parameter specifying the name of the output column.
    """

    output_col = ParamInfo(
        "outputCol",
        "Name of the output column",
        is_optional=False,
        type_converter=TypeConverters.to_string)

    def set_output_col(self, v: str) -> 'HasOutputCol':
        return super().set(self.output_col, v)

    def get_output_col(self) -> str:
        return super().get(self.output_col)


class HasPredictionCol(WithParams):
    """
    An interface for classes with a parameter specifying the column name of the prediction.
    """
    prediction_col = ParamInfo(
        "predictionCol",
        "Column name of prediction.",
        is_optional=False,
        type_converter=TypeConverters.to_string)

    def set_prediction_col(self, v: str) -> 'HasPredictionCol':
        return super().set(self.prediction_col, v)

    def get_prediction_col(self) -> str:
        return super().get(self.prediction_col)
