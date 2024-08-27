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

package org.apache.flink.streaming.api.functions.query;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Internal operator handling queryable AppendingState instances.
 *
 * @param <IN> Input type
 */
@Internal
public class QueryableAppendingStateOperator<IN>
        extends AbstractQueryableStateOperator<AppendingState<IN, ?>, IN> {

    public QueryableAppendingStateOperator(
            String registrationName,
            StateDescriptor<? extends AppendingState<IN, ?>, ?> stateDescriptor) {

        super(registrationName, stateDescriptor);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        state.add(element.getValue());
    }
}
