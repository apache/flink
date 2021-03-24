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

package org.apache.flink.table.planner.sinks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkTransformationProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

/**
 * A {@link DynamicTableSink} defining how to emit a {@link org.apache.flink.table.api.Table} to a {@link org.apache.flink.streaming.api.datastream.DataStream}.
 */
@Internal
public class DataStreamDynamicTableSink implements DynamicTableSink {
    private final ChangelogMode changelogMode;
    private final DataType dataType;

    public DataStreamDynamicTableSink(ChangelogMode changelogMode, DataType dataType) {
        this.changelogMode = changelogMode;
        this.dataType = dataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context sinkContext) {
        final DataStructureConverter converter = sinkContext.createDataStructureConverter(dataType);

        return new SinkTransformationProvider() {
            @Override
            public Transformation<Object> getTransformation(SinkTransformationProvider.Context transformationContext) {
                final SinkTransformationOperatorFactory
                        operatorFactory =
                        new SinkTransformationOperatorFactory(
                                dataType,
                                converter,
                                transformationContext.getRowtimeIndex(),
                                transformationContext.isObjectReuseEnabled());

                return new OneInputTransformation<>(
                        transformationContext.getInputTransformation(),
                        "SinkTransformation",
                        operatorFactory,
                        ExternalTypeInfo.of(dataType, transformationContext.isObjectReuseEnabled()),
                        transformationContext.getInputTransformation().getParallelism());
            }
        };
    }

    @Override
    public DynamicTableSink copy() {
        return new DataStreamDynamicTableSink(changelogMode, dataType);
    }

    @Override
    public String asSummaryString() {
        return toString();
    }

    /**
     * Operator to transform records during conversion to {@link org.apache.flink.streaming.api.datastream.DataStream}.
     *
     * <p>This operator takes care of setting the appropriate rowtime into the output {@link StreamRecord}.
     * It also automatically "unwraps" the incoming record if it has arity 1 and the requested outbound type is a primitive.
     */
    private static class SinkTransformationOperator extends TableStreamOperator<Object>
            implements OneInputStreamOperator<RowData, Object> {

        private final DataType dataType;
        private final DataStructureConverter converter;
        private final int rowtimeIndex;
        private final boolean objectReuseEnabled;

        private final StreamRecord<Object> outElement = new StreamRecord<>(null);

        private boolean unwrapAsPrimitive = false;
        private RowData.FieldGetter singleFieldGetter;

        public SinkTransformationOperator(
                DataType dataType,
                DataStructureConverter converter,
                int rowtimeIndex,
                boolean objectReuseEnabled) {
            this.dataType = dataType;
            this.converter = converter;
            this.rowtimeIndex = rowtimeIndex;
            this.objectReuseEnabled = objectReuseEnabled;
        }

        @Override
        public void open() throws Exception {
            super.open();

            unwrapAsPrimitive = !LogicalTypeChecks.isCompositeType(dataType.getLogicalType());
            singleFieldGetter = RowData.createFieldGetter(dataType.getLogicalType(), 0);

            final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            converter.open(RuntimeConverter.Context.create(classLoader));
        }

        @Override
        public void processElement(StreamRecord<RowData> inElement) throws Exception {
            if (rowtimeIndex != -1) {
                final long rowTimestamp =
                        inElement.getValue().getTimestamp(rowtimeIndex, 3).getMillisecond();
                outElement.setTimestamp(rowTimestamp);
            }

            final RowData inValue = inElement.getValue();
            if (unwrapAsPrimitive) {
                if (inValue.getArity() != 1) {
                    throw new TableException(String.format(
                            "Row '%s' cannot be converted into primitive type '%s' because the received row has arity %d",
                            inValue,
                            dataType.getLogicalType(),
                            inValue.getArity()));
                }

                outElement.replace(singleFieldGetter.getFieldOrNull(inValue));
            } else {
                outElement.replace((inValue));
            }

            if (objectReuseEnabled) {
                outElement.replace(converter.toExternal(inValue));
            }

            output.collect(outElement);
        }
    }


    /** Operator factory for {@link SinkTransformationOperator}. */
    private static class SinkTransformationOperatorFactory
            extends AbstractStreamOperatorFactory<Object> implements
            OneInputStreamOperatorFactory<RowData, Object> {
        private final DataType dataType;
        private final DataStructureConverter converter;
        private final int rowtimeIndex;
        private final boolean objectReuseEnabled;

        public SinkTransformationOperatorFactory(
                DataType dataType,
                DataStructureConverter converter,
                int rowtimeIndex,
                boolean objectReuseEnabled) {
            this.dataType = dataType;
            this.converter = converter;
            this.rowtimeIndex = rowtimeIndex;
            this.objectReuseEnabled = objectReuseEnabled;
        }

        @Override
        public <T extends StreamOperator<Object>> T createStreamOperator(StreamOperatorParameters<Object> parameters) {
            final SinkTransformationOperator operator =
                    new SinkTransformationOperator(
                            dataType,
                            converter,
                            rowtimeIndex,
                            objectReuseEnabled);
            operator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            operator.setProcessingTimeService(processingTimeService);

            return (T) operator;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return SinkTransformationOperator.class;
        }
    }

}
