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

from pyflink import keyword
from pyflink.java_gateway import get_gateway
from pyflink.ml.api import JavaTransformer, Transformer, Estimator, Model, \
    MLEnvironmentFactory, Pipeline
from pyflink.ml.api.param import WithParams, ParamInfo, TypeConverters
from pyflink.ml.lib.param.colname import HasSelectedCols, \
    HasPredictionCol, HasOutputCol
from pyflink.table.types import DataTypes
from pyflink.testing import source_sink_utils
from pyflink.testing.test_case_utils import MLTestCase, exec_insert_table


class HasVectorCol(WithParams):
    """
    Trait for parameter vectorColName.
    """
    vector_col = ParamInfo(
        "vectorCol",
        "Name of a vector column",
        is_optional=False,
        type_converter=TypeConverters.to_string)

    def set_vector_col(self, v: str) -> 'HasVectorCol':
        return super().set(self.vector_col, v)

    def get_vector_col(self) -> str:
        return super().get(self.vector_col)


class WrapperTransformer(JavaTransformer, HasSelectedCols):
    """
    A Transformer wrappers Java Transformer.
    """
    @keyword
    def __init__(self, *, selected_cols=None):
        _j_obj = get_gateway().jvm.org.apache.flink.ml.pipeline.\
            UserDefinedPipelineStages.SelectColumnTransformer()
        super().__init__(_j_obj)
        kwargs = self._input_kwargs
        self._set(**kwargs)


class PythonAddTransformer(Transformer, HasSelectedCols, HasOutputCol):
    """
    A Transformer which is implemented with Python. Output a column
    contains the sum of all columns.
    """
    @keyword
    def __init__(self, *, selected_cols=None, output_col=None):
        super().__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def transform(self, table_env, table):
        input_columns = self.get_selected_cols()
        expr = "+".join(input_columns)
        expr = expr + " as " + self.get_output_col()
        return table.add_columns(expr)


class PythonEstimator(Estimator, HasVectorCol, HasPredictionCol):

    def __init__(self):
        super().__init__()

    def fit(self, table_env, table):
        return PythonModel(
            table_env,
            table.select("max(features) as max_sum"),
            self.get_prediction_col())


class PythonModel(Model):

    def __init__(self, table_env, model_data_table, output_col_name):
        self._model_data_table = model_data_table
        self._output_col_name = output_col_name
        self.max_sum = 0
        self.load_model(table_env)

    def load_model(self, table_env):
        """
        Train the model to get the max_sum value which is used to predict data.
        """
        table_sink = source_sink_utils.TestRetractSink(["max_sum"], [DataTypes.BIGINT()])
        table_env.register_table_sink("Model_Results", table_sink)
        exec_insert_table(self._model_data_table, "Model_Results")
        actual = source_sink_utils.results()
        self.max_sum = actual.apply(0)

    def transform(self, table_env, table):
        """
        Use max_sum to predict input. Return turn if input value is bigger than max_sum
        """
        return table\
            .add_columns("features > {} as {}".format(self.max_sum, self._output_col_name))\
            .select("{}".format(self._output_col_name))


class PythonPipelineTest(MLTestCase):

    def test_java_transformer(self):
        t_env = MLEnvironmentFactory().get_default().get_stream_table_environment()

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        t_env.register_table_sink("TransformerResults", table_sink)

        source_table = t_env.from_elements([(1, 2, 3, 4), (4, 3, 2, 1)], ['a', 'b', 'c', 'd'])
        transformer = WrapperTransformer(selected_cols=["a", "b"])
        exec_insert_table(transformer.transform(t_env, source_table), "TransformerResults")
        actual = source_sink_utils.results()
        self.assert_equals(actual, ["1,2", "4,3"])

    def test_pipeline(self):
        t_env = MLEnvironmentFactory().get_default().get_stream_table_environment()
        train_table = t_env.from_elements(
            [(1, 2), (1, 4), (1, 0), (10, 2), (10, 4), (10, 0)], ['a', 'b'])
        serving_table = t_env.from_elements([(0, 0), (12, 3)], ['a', 'b'])

        table_sink = source_sink_utils.TestAppendSink(
            ['predict_result'],
            [DataTypes.BOOLEAN()])
        t_env.register_table_sink("PredictResults", table_sink)

        # transformer, output features column which is the sum of a and b.
        transformer = PythonAddTransformer(selected_cols=["a", "b"], output_col="features")

        # estimator
        estimator = PythonEstimator()\
            .set_vector_col("features")\
            .set_prediction_col("predict_result")

        # pipeline
        pipeline = Pipeline().append_stage(transformer).append_stage(estimator)
        exec_insert_table(pipeline.fit(t_env, train_table).transform(t_env, serving_table),
                          'PredictResults')

        actual = source_sink_utils.results()
        # the first input is false since 0 + 0 is smaller than the max_sum 14.
        # the second input is true since 12 + 3 is bigger than the max_sum 14.
        self.assert_equals(actual, ["false", "true"])

    def test_pipeline_from_and_to_java_json(self):
        # json generated from Java api
        java_json = '[{"stageClassName":"org.apache.flink.ml.pipeline.' \
                    'UserDefinedPipelineStages$SelectColumnTransformer",' \
                    '"stageJson":"{\\"selectedCols\\":\\"[\\\\\\"a\\\\\\",' \
                    '\\\\\\"b\\\\\\"]\\"}"}]'

        # load json
        p = Pipeline()
        p.load_json(java_json)
        python_json = p.to_json()

        t_env = MLEnvironmentFactory().get_default().get_stream_table_environment()

        table_sink = source_sink_utils.TestAppendSink(
            ['a', 'b'], [DataTypes.BIGINT(), DataTypes.BIGINT()])
        t_env.register_table_sink("TestJsonResults", table_sink)

        source_table = t_env.from_elements([(1, 2, 3, 4), (4, 3, 2, 1)], ['a', 'b', 'c', 'd'])
        transformer = p.get_stages()[0]
        exec_insert_table(transformer.transform(t_env, source_table), "TestJsonResults")

        actual = source_sink_utils.results()

        self.assert_equals(actual, ["1,2", "4,3"])
        self.assertEqual(python_json, java_json)
