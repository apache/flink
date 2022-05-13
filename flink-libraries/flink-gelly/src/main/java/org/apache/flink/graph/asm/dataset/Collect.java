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

import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.AnalyticHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Collect the elements of a {@link DataSet} into a {@link List}.
 *
 * @param <T> element type
 */
public class Collect<T> extends DataSetAnalyticBase<T, List<T>> {

    private static final String COLLECT = "collect";

    private CollectHelper<T> collectHelper;

    private TypeSerializer<T> serializer;

    @Override
    public Collect<T> run(DataSet<T> input) throws Exception {
        super.run(input);

        serializer = input.getType().createSerializer(env.getConfig());

        collectHelper = new CollectHelper<>(serializer);

        input.output(collectHelper).name("Collect");

        return this;
    }

    @Override
    public List<T> getResult() {
        ArrayList<byte[]> accResult = collectHelper.getAccumulator(env, COLLECT);
        if (accResult != null) {
            try {
                return SerializedListAccumulator.deserializeList(accResult, serializer);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Cannot find type class of collected data type", e);
            } catch (IOException e) {
                throw new RuntimeException(
                        "Serialization error while deserializing collected data", e);
            }
        } else {
            throw new RuntimeException("Unable to retrieve the DataSet");
        }
    }

    /**
     * Helper class to collect elements into a serialized list.
     *
     * @param <U> element type
     */
    private static class CollectHelper<U> extends AnalyticHelper<U> {
        private SerializedListAccumulator<U> accumulator;

        private final TypeSerializer<U> serializer;

        public CollectHelper(TypeSerializer<U> serializer) {
            this.serializer = serializer;
        }

        @Override
        public void open(int taskNumber, int numTasks) {
            this.accumulator = new SerializedListAccumulator<>();
        }

        @Override
        public void writeRecord(U record) throws IOException {
            accumulator.add(record, serializer);
        }

        @Override
        public void close() throws IOException {
            addAccumulator(COLLECT, accumulator);
        }
    }
}
