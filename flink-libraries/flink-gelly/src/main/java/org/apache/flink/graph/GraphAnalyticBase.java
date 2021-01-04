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

package org.apache.flink.graph;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.api.common.ExecutionConfig.PARALLELISM_DEFAULT;

/**
 * Base class for {@link GraphAnalytic}.
 *
 * @param <K> key type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 * @param <T> the return type
 */
public abstract class GraphAnalyticBase<K, VV, EV, T> implements GraphAnalytic<K, VV, EV, T> {

    protected ExecutionEnvironment env;

    protected int parallelism = PARALLELISM_DEFAULT;

    @Override
    public GraphAnalytic<K, VV, EV, T> run(Graph<K, VV, EV> input) throws Exception {
        env = input.getContext();
        return this;
    }

    /**
     * Set the parallelism for this analytic's operators. This parameter is necessary because
     * processing a small amount of data with high operator parallelism is slow and wasteful with
     * memory and buffers.
     *
     * <p>Operator parallelism should be set to this given value unless processing asymptotically
     * more data, in which case the default job parallelism should be inherited.
     *
     * @param parallelism operator parallelism
     * @return this
     */
    public GraphAnalyticBase<K, VV, EV, T> setParallelism(int parallelism) {
        Preconditions.checkArgument(
                parallelism > 0 || parallelism == PARALLELISM_DEFAULT,
                "The parallelism must be at least one, or ExecutionConfig.PARALLELISM_DEFAULT (use system default).");

        this.parallelism = parallelism;

        return this;
    }

    @Override
    public T execute() throws Exception {
        env.execute();
        return getResult();
    }

    @Override
    public T execute(String jobName) throws Exception {
        Preconditions.checkNotNull(jobName);

        env.execute(jobName);
        return getResult();
    }
}
