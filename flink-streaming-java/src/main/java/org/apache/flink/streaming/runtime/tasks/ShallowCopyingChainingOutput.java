/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

final class ShallowCopyingChainingOutput<T> extends ChainingOutput<T> {
    public ShallowCopyingChainingOutput(
            Input<T> input,
            @Nullable Counter prevNumRecordsOut,
            OperatorMetricGroup curOperatorMetricGroup,
            @Nullable OutputTag<T> outputTag) {
        super(input, prevNumRecordsOut, curOperatorMetricGroup, outputTag);
    }

    @Override
    protected <X> void pushToOperator(StreamRecord<X> record) {
        try {
            // we know that the given outputTag matches our OutputTag so the record
            // must be of the type that our operator (and Serializer) expects.
            @SuppressWarnings("unchecked")
            StreamRecord<T> castRecord = (StreamRecord<T>) record;

            numRecordsOut.inc();
            numRecordsIn.inc();
            recordProcessor.accept(castRecord.copy(castRecord.getValue()));
        } catch (ClassCastException e) {
            if (outputTag != null) {
                // Enrich error message
                ClassCastException replace =
                        new ClassCastException(
                                String.format(
                                        "%s. Failed to push OutputTag with id '%s' to operator. "
                                                + "This can occur when multiple OutputTags with different types "
                                                + "but identical names are being used.",
                                        e.getMessage(), outputTag.getId()));

                throw new ExceptionInChainedOperatorException(replace);
            } else {
                throw new ExceptionInChainedOperatorException(e);
            }
        } catch (Exception e) {
            throw new ExceptionInChainedOperatorException(e);
        }
    }
}
