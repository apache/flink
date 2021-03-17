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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.util.SuccessException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/** A {@link RichSinkFunction} that verifies that no duplicate records are generated. */
public class ValidatingExactlyOnceSink extends RichSinkFunction<Integer>
        implements ListCheckpointed<Tuple2<Integer, BitSet>> {

    private static final Logger LOG = LoggerFactory.getLogger(ValidatingExactlyOnceSink.class);

    private static final long serialVersionUID = 1748426382527469932L;

    private final int numElementsTotal;

    private BitSet duplicateChecker = new BitSet(); // this is checkpointed

    private int numElements; // this is checkpointed

    public ValidatingExactlyOnceSink(int numElementsTotal) {
        this.numElementsTotal = numElementsTotal;
    }

    @Override
    public void invoke(Integer value) throws Exception {
        numElements++;

        if (duplicateChecker.get(value)) {
            throw new Exception("Received a duplicate: " + value);
        }
        duplicateChecker.set(value);
        if (numElements == numElementsTotal) {
            // validate
            if (duplicateChecker.cardinality() != numElementsTotal) {
                throw new Exception("Duplicate checker has wrong cardinality");
            } else if (duplicateChecker.nextClearBit(0) != numElementsTotal) {
                throw new Exception("Received sparse sequence");
            } else {
                throw new SuccessException();
            }
        }
    }

    @Override
    public List<Tuple2<Integer, BitSet>> snapshotState(long checkpointId, long timestamp)
            throws Exception {
        LOG.info("Snapshot of counter " + numElements + " at checkpoint " + checkpointId);
        return Collections.singletonList(new Tuple2<>(numElements, duplicateChecker));
    }

    @Override
    public void restoreState(List<Tuple2<Integer, BitSet>> state) throws Exception {
        if (state.isEmpty() || state.size() > 1) {
            throw new RuntimeException(
                    "Test failed due to unexpected recovered state size " + state.size());
        }

        Tuple2<Integer, BitSet> s = state.get(0);
        LOG.info("restoring num elements to {}", s.f0);
        this.numElements = s.f0;
        this.duplicateChecker = s.f1;
    }
}
