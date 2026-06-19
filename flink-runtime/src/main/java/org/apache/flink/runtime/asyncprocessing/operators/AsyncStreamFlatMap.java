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

package org.apache.flink.runtime.asyncprocessing.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link AbstractAsyncStateStreamOperator} for executing {@link FlatMapFunction
 * FlatMapFunctions}.
 */
@Internal
public class AsyncStreamFlatMap<IN, OUT>
        extends AbstractAsyncStateUdfStreamOperator<OUT, FlatMapFunction<IN, OUT>>
        implements OneInputStreamOperator<IN, OUT> {

    private static final long serialVersionUID = 1L;

    private transient DeclarationContext declarationContext;

    private transient TimestampedCollectorWithDeclaredVariable<OUT> collector;

    public AsyncStreamFlatMap(FlatMapFunction<IN, OUT> flatMapper) {
        super(flatMapper);
    }

    @Override
    public void open() throws Exception {
        super.open();
        declarationContext = new DeclarationContext(getDeclarationManager());
        collector = new TimestampedCollectorWithDeclaredVariable<>(output, declarationContext);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        collector.setTimestamp(element);
        userFunction.flatMap(element.getValue(), collector);
    }
}
