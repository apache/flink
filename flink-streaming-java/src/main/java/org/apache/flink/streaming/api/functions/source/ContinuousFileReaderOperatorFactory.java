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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

/** {@link ContinuousFileReaderOperator} factory. */
public class ContinuousFileReaderOperatorFactory<OUT, T extends TimestampedInputSplit>
        extends AbstractStreamOperatorFactory<OUT>
        implements YieldingOperatorFactory<OUT>, OneInputStreamOperatorFactory<T, OUT> {

    private final InputFormat<OUT, ? super T> inputFormat;
    private TypeInformation<OUT> type;
    private ExecutionConfig executionConfig;

    public ContinuousFileReaderOperatorFactory(InputFormat<OUT, ? super T> inputFormat) {
        this(inputFormat, null, null);
    }

    public ContinuousFileReaderOperatorFactory(
            InputFormat<OUT, ? super T> inputFormat,
            TypeInformation<OUT> type,
            ExecutionConfig executionConfig) {
        this.inputFormat = inputFormat;
        this.type = type;
        this.executionConfig = executionConfig;
        this.chainingStrategy = ChainingStrategy.HEAD;
    }

    @Override
    public <O extends StreamOperator<OUT>> O createStreamOperator(
            StreamOperatorParameters<OUT> parameters) {
        ContinuousFileReaderOperator<OUT, T> operator =
                new ContinuousFileReaderOperator<>(
                        inputFormat, processingTimeService, getMailboxExecutor());
        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());
        operator.setOutputType(type, executionConfig);
        return (O) operator;
    }

    @Override
    public void setOutputType(TypeInformation<OUT> type, ExecutionConfig executionConfig) {
        this.type = type;
        this.executionConfig = executionConfig;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return ContinuousFileReaderOperator.class;
    }

    @Override
    public boolean isOutputTypeConfigurable() {
        return true;
    }
}
