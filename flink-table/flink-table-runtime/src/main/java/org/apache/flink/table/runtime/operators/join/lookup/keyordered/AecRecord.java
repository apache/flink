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

package org.apache.flink.table.runtime.operators.join.lookup.keyordered;

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Objects;

/** The record used in {@link TableAsyncExecutionController} to contain info about {@link Epoch}. */
public class AecRecord<IN, OUT> {

    private StreamRecord<IN> record;

    private Epoch<OUT> epoch;

    // index where this record is from
    // start with 0
    private int inputIndex;

    public AecRecord() {
        this.record = null;
        this.epoch = null;
        this.inputIndex = -1;
    }

    public AecRecord(StreamRecord<IN> record, Epoch<OUT> epoch, int inputIndex) {
        this.record = record;
        this.epoch = epoch;
        this.inputIndex = inputIndex;
    }

    public AecRecord<IN, OUT> reset(StreamRecord<IN> record, Epoch<OUT> epoch, int inputIndex) {
        this.record = record;
        this.epoch = epoch;
        this.inputIndex = inputIndex;
        return this;
    }

    public AecRecord<IN, OUT> setRecord(StreamRecord<IN> record) {
        this.record = record;
        return this;
    }

    public AecRecord<IN, OUT> setEpoch(Epoch<OUT> epoch) {
        this.epoch = epoch;
        return this;
    }

    public StreamRecord<IN> getRecord() {
        return record;
    }

    public Epoch<OUT> getEpoch() {
        return epoch;
    }

    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public String toString() {
        return "AecRecord{"
                + "record="
                + record
                + ", epoch="
                + epoch
                + ", inputIndex="
                + inputIndex
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AecRecord<?, ?> aecRecord = (AecRecord<?, ?>) o;
        return inputIndex == aecRecord.inputIndex
                && Objects.equals(record, aecRecord.record)
                && Objects.equals(epoch, aecRecord.epoch);
    }

    @Override
    public int hashCode() {
        return Objects.hash(record, epoch, inputIndex);
    }
}
