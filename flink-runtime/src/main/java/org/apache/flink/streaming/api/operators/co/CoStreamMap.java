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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * {@link org.apache.flink.streaming.api.operators.StreamOperator} for processing {@link
 * CoMapFunction CoMapFunctions}.
 */
@Internal
public class CoStreamMap<IN1, IN2, OUT>
        extends AbstractUdfStreamOperator<OUT, CoMapFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT> {

    private static final long serialVersionUID = 1L;

    public CoStreamMap(CoMapFunction<IN1, IN2, OUT> mapper) {
        super(mapper);
    }

    @Override
    public void processElement1(StreamRecord<IN1> element) throws Exception {
        output.collect(element.replace(userFunction.map1(element.getValue())));
    }

    @Override
    public void processElement2(StreamRecord<IN2> element) throws Exception {
        output.collect(element.replace(userFunction.map2(element.getValue())));
    }
}
