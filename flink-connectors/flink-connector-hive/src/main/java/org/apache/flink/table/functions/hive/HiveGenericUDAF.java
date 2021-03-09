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

package org.apache.flink.table.functions.hive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.functions.hive.conversion.IdentityConversion;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Arrays;

/**
 * An {@link AggregateFunction} implementation that calls Hive's {@link UDAF} or {@link
 * GenericUDAFEvaluator}.
 */
@Internal
public class HiveGenericUDAF
        extends AggregateFunction<Object, GenericUDAFEvaluator.AggregationBuffer>
        implements HiveFunction {

    private final HiveFunctionWrapper hiveFunctionWrapper;
    // Flag that indicates whether a bridge between GenericUDAF and UDAF is required.
    // Old UDAF can be used with the GenericUDAF infrastructure through bridging.
    private final boolean isUDAFBridgeRequired;

    private Object[] constantArguments;
    private DataType[] argTypes;

    private transient GenericUDAFEvaluator partialEvaluator;
    private transient GenericUDAFEvaluator finalEvaluator;
    private transient ObjectInspector partialResultObjectInspector;
    private transient ObjectInspector finalResultObjectInspector;
    private transient HiveObjectConversion[] conversions;
    private transient boolean allIdentityConverter;
    private transient boolean initialized;

    private final HiveShim hiveShim;

    public HiveGenericUDAF(HiveFunctionWrapper funcWrapper, HiveShim hiveShim) {
        this(funcWrapper, false, hiveShim);
    }

    public HiveGenericUDAF(
            HiveFunctionWrapper funcWrapper, boolean isUDAFBridgeRequired, HiveShim hiveShim) {
        this.hiveFunctionWrapper = funcWrapper;
        this.isUDAFBridgeRequired = isUDAFBridgeRequired;
        this.hiveShim = hiveShim;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        init();
    }

    private void init() throws HiveException {
        ObjectInspector[] inputInspectors =
                HiveInspectors.toInspectors(hiveShim, constantArguments, argTypes);

        // Flink UDAF only supports Hive UDAF's PARTIAL_1 and FINAL mode

        // PARTIAL1: from original data to partial aggregation data:
        // 		iterate() and terminatePartial() will be called.
        this.partialEvaluator = createEvaluator(inputInspectors);
        this.partialResultObjectInspector =
                partialEvaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputInspectors);

        // FINAL: from partial aggregation to full aggregation:
        // 		merge() and terminate() will be called.
        this.finalEvaluator = createEvaluator(inputInspectors);
        this.finalResultObjectInspector =
                finalEvaluator.init(
                        GenericUDAFEvaluator.Mode.FINAL,
                        new ObjectInspector[] {partialResultObjectInspector});

        conversions = new HiveObjectConversion[inputInspectors.length];
        for (int i = 0; i < inputInspectors.length; i++) {
            conversions[i] =
                    HiveInspectors.getConversion(
                            inputInspectors[i], argTypes[i].getLogicalType(), hiveShim);
        }
        allIdentityConverter =
                Arrays.stream(conversions).allMatch(conv -> conv instanceof IdentityConversion);

        initialized = true;
    }

    private GenericUDAFEvaluator createEvaluator(ObjectInspector[] inputInspectors)
            throws SemanticException {
        GenericUDAFResolver2 resolver;

        if (isUDAFBridgeRequired) {
            resolver = new GenericUDAFBridge((UDAF) hiveFunctionWrapper.createFunction());
        } else {
            resolver = (GenericUDAFResolver2) hiveFunctionWrapper.createFunction();
        }

        return resolver.getEvaluator(
                hiveShim.createUDAFParameterInfo(
                        inputInspectors,
                        // The flag to indicate if the UDAF invocation was from the windowing
                        // function call or not.
                        // TODO: investigate whether this has impact on Flink streaming job with
                        // windows
                        Boolean.FALSE,
                        // Returns true if the UDAF invocation was qualified with DISTINCT keyword.
                        // Note that this is provided for informational purposes only and the
                        // function implementation
                        // is not expected to ensure the distinct property for the parameter values.
                        // That is handled by the framework.
                        Boolean.FALSE,
                        // Returns true if the UDAF invocation was done via the wildcard syntax
                        // FUNCTION(*).
                        // Note that this is provided for informational purposes only and the
                        // function implementation
                        // is not expected to ensure the wildcard handling of the target relation.
                        // That is handled by the framework.
                        Boolean.FALSE));
    }

    /**
     * This is invoked without calling open() in Blink, so we need to call init() for
     * getNewAggregationBuffer(). TODO: re-evaluate how this will fit into Flink's new type
     * inference and udf systemß
     */
    @Override
    public GenericUDAFEvaluator.AggregationBuffer createAccumulator() {
        try {
            if (!initialized) {
                init();
            }
            return partialEvaluator.getNewAggregationBuffer();
        } catch (Exception e) {
            throw new FlinkHiveUDFException(
                    String.format(
                            "Failed to create accumulator for %s",
                            hiveFunctionWrapper.getClassName()),
                    e);
        }
    }

    public void accumulate(GenericUDAFEvaluator.AggregationBuffer acc, Object... inputs)
            throws HiveException {
        if (!allIdentityConverter) {
            for (int i = 0; i < inputs.length; i++) {
                inputs[i] = conversions[i].toHiveObject(inputs[i]);
            }
        }

        partialEvaluator.iterate(acc, inputs);
    }

    public void merge(
            GenericUDAFEvaluator.AggregationBuffer accumulator,
            Iterable<GenericUDAFEvaluator.AggregationBuffer> its)
            throws HiveException {

        for (GenericUDAFEvaluator.AggregationBuffer buffer : its) {
            finalEvaluator.merge(accumulator, partialEvaluator.terminatePartial(buffer));
        }
    }

    @Override
    public Object getValue(GenericUDAFEvaluator.AggregationBuffer accumulator) {
        try {
            return HiveInspectors.toFlinkObject(
                    finalResultObjectInspector, finalEvaluator.terminate(accumulator), hiveShim);
        } catch (HiveException e) {
            throw new FlinkHiveUDFException(
                    String.format(
                            "Failed to get final result on %s", hiveFunctionWrapper.getClassName()),
                    e);
        }
    }

    @Override
    public void setArgumentTypesAndConstants(Object[] constantArguments, DataType[] argTypes) {
        this.constantArguments = constantArguments;
        this.argTypes = argTypes;
    }

    @Override
    public DataType getHiveResultType(Object[] constantArguments, DataType[] argTypes) {
        try {
            if (!initialized) {
                setArgumentTypesAndConstants(constantArguments, argTypes);
                init();
            }

            return HiveTypeUtil.toFlinkType(finalResultObjectInspector);
        } catch (Exception e) {
            throw new FlinkHiveUDFException(
                    String.format(
                            "Failed to get Hive result type from %s",
                            hiveFunctionWrapper.getClassName()),
                    e);
        }
    }

    @Override
    public TypeInformation getResultType() {
        return TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(
                getHiveResultType(this.constantArguments, this.argTypes));
    }

    @Override
    public TypeInformation<GenericUDAFEvaluator.AggregationBuffer> getAccumulatorType() {
        return new GenericTypeInfo<>(GenericUDAFEvaluator.AggregationBuffer.class);
    }
}
