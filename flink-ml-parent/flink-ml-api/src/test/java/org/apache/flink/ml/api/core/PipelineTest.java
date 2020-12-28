/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.ml.api.core;

import org.apache.flink.ml.api.misc.param.ParamInfo;
import org.apache.flink.ml.api.misc.param.ParamInfoFactory;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests the behavior of {@link Pipeline}. */
public class PipelineTest {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testPipelineBehavior() {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new MockTransformer("a"));
        pipeline.appendStage(new MockEstimator("b"));
        pipeline.appendStage(new MockEstimator("c"));
        pipeline.appendStage(new MockTransformer("d"));
        assert describePipeline(pipeline).equals("a_b_c_d");

        Pipeline pipelineModel = pipeline.fit(null, null);
        assert describePipeline(pipelineModel).equals("a_mb_mc_d");

        thrown.expect(RuntimeException.class);
        thrown.expectMessage("Pipeline contains Estimator, need to fit first.");
        pipeline.transform(null, null);
    }

    @Test
    public void testPipelineRestore() {
        Pipeline pipeline = new Pipeline();
        pipeline.appendStage(new MockTransformer("a"));
        pipeline.appendStage(new MockEstimator("b"));
        pipeline.appendStage(new MockEstimator("c"));
        pipeline.appendStage(new MockTransformer("d"));
        String pipelineJson = pipeline.toJson();

        Pipeline restoredPipeline = new Pipeline(pipelineJson);
        assert describePipeline(restoredPipeline).equals("a_b_c_d");

        Pipeline pipelineModel = pipeline.fit(null, null);
        String modelJson = pipelineModel.toJson();

        Pipeline restoredPipelineModel = new Pipeline(modelJson);
        assert describePipeline(restoredPipelineModel).equals("a_mb_mc_d");
    }

    private static String describePipeline(Pipeline p) {
        StringBuilder res = new StringBuilder();
        for (PipelineStage s : p.getStages()) {
            if (res.length() != 0) {
                res.append("_");
            }
            res.append(((SelfDescribe) s).describe());
        }
        return res.toString();
    }

    /** Interface to describe a class with a string, only for pipeline test. */
    private interface SelfDescribe {
        ParamInfo<String> DESCRIPTION =
                ParamInfoFactory.createParamInfo("description", String.class).build();

        String describe();
    }

    /** Mock estimator for pipeline test. */
    public static class MockEstimator implements Estimator<MockEstimator, MockModel>, SelfDescribe {
        private final Params params = new Params();

        public MockEstimator() {}

        MockEstimator(String description) {
            set(DESCRIPTION, description);
        }

        @Override
        public MockModel fit(TableEnvironment tEnv, Table input) {
            return new MockModel("m" + describe());
        }

        @Override
        public Params getParams() {
            return params;
        }

        @Override
        public String describe() {
            return get(DESCRIPTION);
        }
    }

    /** Mock transformer for pipeline test. */
    public static class MockTransformer implements Transformer<MockTransformer>, SelfDescribe {
        private final Params params = new Params();

        public MockTransformer() {}

        MockTransformer(String description) {
            set(DESCRIPTION, description);
        }

        @Override
        public Table transform(TableEnvironment tEnv, Table input) {
            return input;
        }

        @Override
        public Params getParams() {
            return params;
        }

        @Override
        public String describe() {
            return get(DESCRIPTION);
        }
    }

    /** Mock model for pipeline test. */
    public static class MockModel implements Model<MockModel>, SelfDescribe {
        private final Params params = new Params();

        public MockModel() {}

        MockModel(String description) {
            set(DESCRIPTION, description);
        }

        @Override
        public Table transform(TableEnvironment tEnv, Table input) {
            return input;
        }

        @Override
        public Params getParams() {
            return params;
        }

        @Override
        public String describe() {
            return get(DESCRIPTION);
        }
    }
}
