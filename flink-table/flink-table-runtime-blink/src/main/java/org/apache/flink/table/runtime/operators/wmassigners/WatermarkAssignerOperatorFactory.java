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

package org.apache.flink.table.runtime.operators.wmassigners;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedWatermarkGenerator;
import org.apache.flink.table.runtime.generated.WatermarkGenerator;

/** The factory of {@link WatermarkAssignerOperator}. */
public class WatermarkAssignerOperatorFactory extends AbstractStreamOperatorFactory<RowData>
        implements OneInputStreamOperatorFactory<RowData, RowData> {

    private static final long serialVersionUID = 1L;

    private final int rowtimeFieldIndex;

    private final long idleTimeout;

    private final GeneratedWatermarkGenerator generatedWatermarkGenerator;

    public WatermarkAssignerOperatorFactory(
            int rowtimeFieldIndex,
            long idleTimeout,
            GeneratedWatermarkGenerator generatedWatermarkGenerator) {
        this.rowtimeFieldIndex = rowtimeFieldIndex;
        this.idleTimeout = idleTimeout;
        this.generatedWatermarkGenerator = generatedWatermarkGenerator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public StreamOperator createStreamOperator(StreamOperatorParameters initializer) {
        WatermarkGenerator watermarkGenerator =
                generatedWatermarkGenerator.newInstance(
                        initializer.getContainingTask().getUserCodeClassLoader());
        WatermarkAssignerOperator operator =
                new WatermarkAssignerOperator(
                        rowtimeFieldIndex, watermarkGenerator, idleTimeout, processingTimeService);
        operator.setup(
                initializer.getContainingTask(),
                initializer.getStreamConfig(),
                initializer.getOutput());
        return operator;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return WatermarkAssignerOperator.class;
    }
}
