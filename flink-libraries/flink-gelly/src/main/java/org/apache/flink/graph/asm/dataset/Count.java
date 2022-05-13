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

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.AnalyticHelper;

import java.io.IOException;

/**
 * Count the number of elements in a {@link DataSet}.
 *
 * @param <T> element type
 */
public class Count<T> extends DataSetAnalyticBase<T, Long> {

    private static final String COUNT = "count";

    private CountHelper<T> countHelper;

    @Override
    public Count<T> run(DataSet<T> input) throws Exception {
        super.run(input);

        countHelper = new CountHelper<>();

        input.output(countHelper).name("Count");

        return this;
    }

    @Override
    public Long getResult() {
        return countHelper.getAccumulator(env, COUNT);
    }

    /**
     * Helper class to count elements.
     *
     * @param <U> element type
     */
    private static class CountHelper<U> extends AnalyticHelper<U> {
        private long count;

        @Override
        public void writeRecord(U record) throws IOException {
            count++;
        }

        @Override
        public void close() throws IOException {
            addAccumulator(COUNT, new LongCounter(count));
        }
    }
}
