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

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * A {@link CoordinationRequestHandler} to test fetching SELECT query results. It will randomly do
 * checkpoint or restart from checkpoint.
 */
public class TestCheckpointedCoordinationRequestHandler<T>
        extends AbstractTestCoordinationRequestHandler<T> {

    private int checkpointCountDown;

    private LinkedList<T> data;
    private List<T> checkpointingData;
    private List<T> checkpointedData;

    private List<T> checkpointingBuffered;
    private List<T> checkpointedBuffered;

    private long checkpointingOffset;

    public TestCheckpointedCoordinationRequestHandler(
            List<T> data, TypeSerializer<T> serializer, String accumulatorName) {
        super(serializer, accumulatorName);
        this.checkpointCountDown = 0;

        this.data = new LinkedList<>(data);
        this.checkpointedData = new ArrayList<>(data);

        this.checkpointedBuffered = new ArrayList<>();

        this.checkpointingOffset = 0;
    }

    @Override
    protected void updateBufferedResults() {
        for (int i = random.nextInt(3) + 1; i > 0; i--) {
            if (checkpointCountDown > 0) {
                // countdown on-going checkpoint
                checkpointCountDown--;
                if (checkpointCountDown == 0) {
                    // complete a checkpoint
                    checkpointedData = checkpointingData;
                    checkpointedBuffered = checkpointingBuffered;
                    checkpointedOffset = checkpointingOffset;
                }
            }

            int r = random.nextInt(10);
            if (r < 6) {
                // with 60% chance we add data
                int size = Math.min(data.size(), BATCH_SIZE * 2 - buffered.size());
                if (size > 0) {
                    size = random.nextInt(size) + 1;
                }
                for (int j = 0; j < size; j++) {
                    buffered.add(data.removeFirst());
                }

                if (data.isEmpty()) {
                    buildAccumulatorResults();
                    closed = true;
                    break;
                }
            } else if (r < 9) {
                // with 30% chance we do a checkpoint completed in the future
                if (checkpointCountDown == 0) {
                    checkpointCountDown = random.nextInt(5) + 1;
                    checkpointingData = new ArrayList<>(data);
                    checkpointingBuffered = new ArrayList<>(buffered);
                    checkpointingOffset = offset;
                }
            } else {
                // with 10% chance we fail
                checkpointCountDown = 0;
                version = UUID.randomUUID().toString();

                // we shuffle data to simulate jobs whose result order is undetermined
                Collections.shuffle(checkpointedData);
                data = new LinkedList<>(checkpointedData);

                buffered = new LinkedList<>(checkpointedBuffered);
                offset = checkpointedOffset;
            }
        }
    }
}
