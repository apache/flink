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

from pyflink.ml.api import JavaTransformer, Transformer, Estimator, Model, \
    Pipeline, JavaEstimator, JavaModel
from pyflink.ml.api.param.base import WithParams, ParamInfo, TypeConverters
from pyflink import keyword
from pyflink.testing.test_case_utils import MLTestCase, PyFlinkTestCase


class PipelineTest(PyFlinkTestCase):

    @staticmethod
    def describe_pipeline(pipeline):
        res = [stage.get_desc() for stage in pipeline.get_stages()]
        return "_".join(res)

    def test_construct_pipeline(self):
        pipeline1 = Pipeline()
        pipeline1.append_stage(MockTransformer(self_desc="a1"))
        pipeline1.append_stage(MockJavaTransformer(self_desc="ja1"))

        pipeline2 = Pipeline()
        pipeline2.append_stage(MockTransformer(self_desc="a2"))
        pipeline2.append_stage(MockJavaTransformer(self_desc="ja2"))

        pipeline3 = Pipeline(pipeline1.get_stages(), pipeline2.to_json())
        self.assertEqual("a1_ja1_a2_ja2", PipelineTest.describe_pipeline(pipeline3))

    def test_pipeline_behavior(self):
        pipeline = Pipeline()
        pipeline.append_stage(MockTransformer(self_desc="a"))
        pipeline.append_stage(MockJavaTransformer(self_desc="ja"))
        pipeline.append_stage(MockEstimator(self_desc="b"))
        pipeline.append_stage(MockJavaEstimator(self_desc="jb"))
        pipeline.append_stage(MockEstimator(self_desc="c"))
        pipeline.append_stage(MockTransformer(self_desc="d"))
        self.assertEqual("a_ja_b_jb_c_d", PipelineTest.describe_pipeline(pipeline))

        pipeline_model = pipeline.fit(None, None)
        self.assertEqual("a_ja_mb_mjb_mc_d", PipelineTest.describe_pipeline(pipeline_model))

    def test_pipeline_restore(self):
        pipeline = Pipeline()
        pipeline.append_stage(MockTransformer(self_desc="a"))
        pipeline.append_stage(MockJavaTransformer(self_desc="ja"))
        pipeline.append_stage(MockEstimator(self_desc="b"))
        pipeline.append_stage(MockJavaEstimator(self_desc="jb"))
        pipeline.append_stage(MockEstimator(self_desc="c"))
        pipeline.append_stage(MockTransformer(self_desc="d"))

        pipeline_new = Pipeline()
        pipeline_new.load_json(pipeline.to_json())
        self.assertEqual("a_ja_b_jb_c_d", PipelineTest.describe_pipeline(pipeline_new))

        pipeline_model = pipeline_new.fit(None, None)
        self.assertEqual("a_ja_mb_mjb_mc_d", PipelineTest.describe_pipeline(pipeline_model))


class ValidationPipelineTest(MLTestCase):

    def test_pipeline_from_invalid_json(self):
        invalid_json = '[a:aa]'

        # load json
        p = Pipeline()
        with self.assertRaises(RuntimeError) as context:
            p.load_json(invalid_json)
        exception_str = str(context.exception)

        # NOTE: only check the general error message since the detailed error message
        # would be different in different environment.
        self.assertTrue(
            'Cannot load the JSON as either a Java Pipeline or a Python Pipeline.'
            in exception_str)
        self.assertTrue('Python Pipeline load failed due to:' in exception_str)
        self.assertTrue('Java Pipeline load failed due to:' in exception_str)
        self.assertTrue('JsonParseException' in exception_str)


class SelfDescribe(WithParams):
    self_desc = ParamInfo("selfDesc", "selfDesc", type_converter=TypeConverters.to_string)

    def set_desc(self, v):
        return super().set(self.self_desc, v)

    def get_desc(self):
        return super().get(self.self_desc)


class MockTransformer(Transformer, SelfDescribe):
    @keyword
    def __init__(self, *, self_desc=None):
        super().__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def transform(self, table_env, table):
        return table


class MockEstimator(Estimator, SelfDescribe):
    @keyword
    def __init__(self, *, self_desc=None):
        super().__init__()
        self._self_desc = self_desc
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def fit(self, table_env, table):
        return MockModel(self_desc="m" + self._self_desc)


class MockModel(Model, SelfDescribe):
    @keyword
    def __init__(self, *, self_desc=None):
        super().__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def transform(self, table_env, table):
        return table


class MockJavaTransformer(JavaTransformer, SelfDescribe):
    @keyword
    def __init__(self, *, self_desc=None):
        super().__init__(None)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def transform(self, table_env, table):
        return table


class MockJavaEstimator(JavaEstimator, SelfDescribe):
    @keyword
    def __init__(self, *, self_desc=None):
        super().__init__(None)
        self._self_desc = self_desc
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def fit(self, table_env, table):
        return MockJavaModel(self_desc="m" + self._self_desc)


class MockJavaModel(JavaModel, SelfDescribe):
    @keyword
    def __init__(self, *, self_desc=None):
        super().__init__(None)
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def transform(self, table_env, table):
        return table
