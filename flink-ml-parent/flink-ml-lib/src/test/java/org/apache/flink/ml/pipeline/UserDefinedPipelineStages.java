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

package org.apache.flink.ml.pipeline;

import org.apache.flink.ml.api.core.Transformer;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.ml.params.shared.colname.HasSelectedCols;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.expressions.Expression;

import java.util.Arrays;

/** Util class for testing {@link org.apache.flink.ml.api.core.PipelineStage}. */
public class UserDefinedPipelineStages {

    /** A {@link Transformer} which is used to perform column selection. */
    public static class SelectColumnTransformer
            implements Transformer<SelectColumnTransformer>,
                    HasSelectedCols<SelectColumnTransformer> {

        private Params params;

        public SelectColumnTransformer() {
            this.params = new Params();
        }

        @Override
        public Table transform(TableEnvironment tEnv, Table input) {
            return input.select(
                    Arrays.stream(this.getSelectedCols())
                            .map(Expressions::$)
                            .toArray(Expression[]::new));
        }

        @Override
        public Params getParams() {
            return params;
        }
    }
}
