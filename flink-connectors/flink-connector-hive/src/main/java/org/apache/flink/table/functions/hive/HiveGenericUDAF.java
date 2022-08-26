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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.functions.hive.conversion.IdentityConversion;
import org.apache.flink.table.functions.hive.util.HiveFunctionUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Arrays;

/**
 * An {@link AggregateFunction} implementation that calls Hive's {@link UDAF} or {@link
 * GenericUDAFEvaluator}.
 */
@Internal
public class HiveGenericUDAF
        extends AggregateFunction<Object, GenericUDAFEvaluator.AggregationBuffer>
        implements HiveFunction<GenericUDAFResolver> {

    private final HiveFunctionWrapper<GenericUDAFResolver> hiveFunctionWrapper;
    // Flag that indicates whether a bridge between GenericUDAF and UDAF is required.
    // Old UDAF can be used with the GenericUDAF infrastructure through bridging.
    private final boolean isUDAFBridgeRequired;
    // Flag that indicates whether the GenericUDAF implement GenericUDAFResolver2
    private final boolean isUDAFResolve2;
    private final HiveShim hiveShim;

    private HiveFunctionArguments arguments;

    private transient GenericUDAFEvaluator partialEvaluator;
    private transient GenericUDAFEvaluator finalEvaluator;
    private transient ObjectInspector finalResultObjectInspector;
    private transient boolean isArgsSingleArray;
    private transient HiveObjectConversion[] conversions;
    private transient boolean allIdentityConverter;
    private transient boolean initialized;

    public HiveGenericUDAF(
            HiveFunctionWrapper<GenericUDAFResolver> funcWrapper,
            boolean isUDAFBridgeRequired,
            boolean isUDAFResolve2,
            HiveShim hiveShim) {
        this.hiveFunctionWrapper = funcWrapper;
        this.isUDAFBridgeRequired = isUDAFBridgeRequired;
        this.isUDAFResolve2 = isUDAFResolve2;
        this.hiveShim = hiveShim;
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        init();
    }

    private void init() throws HiveException {
        ObjectInspector[] inputInspectors = HiveInspectors.getArgInspectors(hiveShim, arguments);

        // Flink UDAF only supports Hive UDAF's PARTIAL_1 and FINAL mode

        // PARTIAL1: from original data to partial aggregation data:
        // 		iterate() and terminatePartial() will be called.
        this.partialEvaluator = createEvaluator(inputInspectors);
        ObjectInspector partialResultObjectInspector =
                partialEvaluator.init(GenericUDAFEvaluator.Mode.PARTIAL1, inputInspectors);

        // FINAL: from partial aggregation to full aggregation:
        // 		merge() and terminate() will be called.
        this.finalEvaluator = createEvaluator(inputInspectors);
        this.finalResultObjectInspector =
                finalEvaluator.init(
                        GenericUDAFEvaluator.Mode.FINAL,
                        new ObjectInspector[] {partialResultObjectInspector});

        isArgsSingleArray = HiveFunctionUtil.isSingleBoxedArray(arguments);
        conversions = new HiveObjectConversion[inputInspectors.length];
        for (int i = 0; i < inputInspectors.length; i++) {
            conversions[i] =
                    HiveInspectors.getConversion(
                            inputInspectors[i],
                            arguments.getDataType(i).getLogicalType(),
                            hiveShim);
        }
        allIdentityConverter =
                Arrays.stream(conversions).allMatch(conv -> conv instanceof IdentityConversion);

        initialized = true;
    }

    public GenericUDAFEvaluator createEvaluator(ObjectInspector[] inputInspectors)
            throws SemanticException {
        SimpleGenericUDAFParameterInfo parameterInfo =
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
                        Boolean.FALSE);

        if (isUDAFBridgeRequired) {
            return new GenericUDAFBridge((UDAF) hiveFunctionWrapper.createFunction())
                    .getEvaluator(parameterInfo);
        } else if (isUDAFResolve2) {
            return ((GenericUDAFResolver2) hiveFunctionWrapper.createFunction())
                    .getEvaluator(parameterInfo);
        } else {
            // otherwise, it's instance of GenericUDAFResolver
            return hiveFunctionWrapper.createFunction().getEvaluator(parameterInfo.getParameters());
        }
    }

    /**
     * This is invoked without calling open(), so we need to call init() for
     * getNewAggregationBuffer(). TODO: re-evaluate how this will fit into Flink's new type
     * inference and udf system
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
                            hiveFunctionWrapper.getUDFClassName()),
                    e);
        }
    }

    public void accumulate(GenericUDAFEvaluator.AggregationBuffer acc, Object... inputs)
            throws HiveException {
        // When the parameter of the function is (Integer, Array[Double]), Flink calls
        // udf.accumulate(AggregationBuffer, Integer, Array[Double]), which is not a problem.
        // But when the parameter is a single array, Flink calls udf.accumulate(AggregationBuffer,
        // Array[Double]), at this point java's var-args will cast Array[Double] to Array[Object]
        // and let it be Object... args, So we need wrap it.
        if (isArgsSingleArray) {
            inputs = new Object[] {inputs};
        }
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
                            "Failed to get final result on %s",
                            hiveFunctionWrapper.getUDFClassName()),
                    e);
        }
    }

    @Override
    public void close() throws Exception {
        partialEvaluator.close();
        finalEvaluator.close();
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInference.newBuilder()
                .inputTypeStrategy(new HiveFunctionInputStrategy(this))
                .accumulatorTypeStrategy(
                        TypeStrategies.explicit(
                                DataTypes.RAW(GenericUDAFEvaluator.AggregationBuffer.class)
                                        .toDataType(typeFactory)))
                .outputTypeStrategy(new HiveFunctionOutputStrategy(this))
                .build();
    }

    @Override
    public void setArguments(CallContext callContext) {
        if (arguments == null) {
            arguments = HiveFunctionArguments.create(callContext);
        }
    }

    @Override
    public DataType inferReturnType() {
        try {
            if (!initialized) {
                init();
            }
            return HiveTypeUtil.toFlinkType(finalResultObjectInspector);
        } catch (Exception e) {
            throw new FlinkHiveUDFException(
                    String.format(
                            "Failed to get Hive result type from %s",
                            hiveFunctionWrapper.getUDFClassName()),
                    e);
        }
    }

    @Override
    public HiveFunctionWrapper<GenericUDAFResolver> getFunctionWrapper() {
        return hiveFunctionWrapper;
    }

    @Override
    public TypeInformation<GenericUDAFEvaluator.AggregationBuffer> getAccumulatorType() {
        return new GenericTypeInfo<>(GenericUDAFEvaluator.AggregationBuffer.class);
    }
}
