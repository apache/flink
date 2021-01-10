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

from abc import ABC, abstractmethod
from pyflink.ml.api import Transformer, Estimator, MLEnvironmentFactory

from pyflink.testing.test_case_utils import PyFlinkTestCase


class PipelineStageTestBase(ABC):
    """
    The base class for testing the base implementation of pipeline stages, i.e. Estimators
    and Transformers. This class is package private because we do not expect extension outside
    of the package.
    """

    @abstractmethod
    def create_pipeline_stage(self):
        pass


class TransformerBaseTest(PipelineStageTestBase, PyFlinkTestCase):
    """
    Test for TransformerBase.
    """
    def test_fit_table(self):
        id = MLEnvironmentFactory.get_new_ml_environment_id()
        env = MLEnvironmentFactory.get(id)
        table = env.get_stream_table_environment().from_elements([(1, 2, 3)])
        transformer = self.create_pipeline_stage()
        transformer.transform(env.get_stream_table_environment(), table)
        self.assertTrue(transformer.transformed)

    def create_pipeline_stage(self):
        return self.FakeTransFormer()

    class FakeTransFormer(Transformer):
        """
        This fake transformer simply record which transform method is invoked.
        """

        def __init__(self):
            self.transformed = False

        def transform(self, table_env, table):
            self.transformed = True
            return table


class EstimatorBaseTest(PipelineStageTestBase, PyFlinkTestCase):
    """
    Test for EstimatorBase.
    """
    def test_fit_table(self):
        id = MLEnvironmentFactory.get_new_ml_environment_id()
        env = MLEnvironmentFactory.get(id)
        table = env.get_stream_table_environment().from_elements([(1, 2, 3)])
        estimator = self.create_pipeline_stage()
        estimator.fit(env.get_stream_table_environment(), table)
        self.assertTrue(estimator.fitted)

    def create_pipeline_stage(self):
        return self.FakeEstimator()

    class FakeEstimator(Estimator):
        """
        This fake estimator simply record which fit method is invoked.
        """

        def __init__(self):
            self.fitted = False

        def fit(self, table_env, table):
            self.fitted = True
            return None
