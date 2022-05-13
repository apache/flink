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

package org.apache.flink.graph.asm.dataset;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Preconditions;

/**
 * Base class for {@link DataSetAnalytic}.
 *
 * @param <T> element type
 * @param <R> the return type
 */
public abstract class DataSetAnalyticBase<T, R> implements DataSetAnalytic<T, R> {

    protected ExecutionEnvironment env;

    @Override
    public DataSetAnalyticBase<T, R> run(DataSet<T> input) throws Exception {
        env = input.getExecutionEnvironment();
        return this;
    }

    @Override
    public R execute() throws Exception {
        env.execute();
        return getResult();
    }

    @Override
    public R execute(String jobName) throws Exception {
        Preconditions.checkNotNull(jobName);

        env.execute(jobName);
        return getResult();
    }
}
