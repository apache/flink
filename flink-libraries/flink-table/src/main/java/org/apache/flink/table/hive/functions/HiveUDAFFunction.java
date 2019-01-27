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

package org.apache.flink.table.hive.functions;

import org.apache.flink.table.api.functions.AggregateFunction;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.GenericRow;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver2;
import org.apache.hadoop.hive.ql.udf.generic.SimpleGenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A Flink Aggregate Function wrapper which wraps a Hive UDAF.
 */
public class HiveUDAFFunction extends AggregateFunction<BaseRow, GenericUDAFEvaluator.AggregationBuffer> {

	private final HiveFunctionWrapper<?> hiveFunctionWrapper;
	private final boolean isUDAFBridgeRequired;

	private transient GenericUDAFResolver2 resolver;
	private transient GenericUDAFEvaluator finalEvaluator;
	private transient boolean finalEvaluatorByVoid;
	private transient GenericUDAFEvaluator partial1Evaluator;
	private transient boolean partial1EvaluatorByVoid;
	private transient GenericUDAFEvaluator partial2Evaluator;
	private transient boolean partial2EvaluatorByVoid;
	private transient ObjectInspector[] inputInspectors;
	private transient ObjectInspector returnInspector;
	private transient ObjectInspector partialResultInspector;

	public HiveUDAFFunction(
			HiveFunctionWrapper<?> hiveFunctionWrapper) throws ClassNotFoundException {

		this.hiveFunctionWrapper = hiveFunctionWrapper;
		this.isUDAFBridgeRequired = hiveFunctionWrapper.getUDFClass().equals(UDAF.class);
		this.finalEvaluatorByVoid = true;
		this.partial1EvaluatorByVoid = true;
		this.partial2EvaluatorByVoid = true;
		this.inputInspectors = null;
	}

	private GenericUDAFResolver2 newResolver()
			throws IllegalAccessException, InstantiationException, ClassNotFoundException {

		if (isUDAFBridgeRequired) {
			return new GenericUDAFBridge(
					(UDAF) hiveFunctionWrapper.createFunction());
		} else {
			return (GenericUDAFResolver2) hiveFunctionWrapper.createFunction();
		}
	}

	public void accumulate(
			GenericUDAFEvaluator.AggregationBuffer acc,
			Object... params) {

		if (null == inputInspectors
				|| finalEvaluatorByVoid
				|| partial1EvaluatorByVoid
				|| partial2EvaluatorByVoid) {

			List<Boolean> constans = new ArrayList<>(params.length);
			for (Object ignored : params) {
				constans.add(false);
			}
			inputInspectors = HiveInspectors.toInspectors(params, constans);

			try {
				if (finalEvaluatorByVoid) {
					finalEvaluator = null;
					getFinalEvaluator();
				}
				if (partial1EvaluatorByVoid) {
					partial1Evaluator = null;
					getPartial1Evaluator();
				}
				if (partial2EvaluatorByVoid) {
					partial2Evaluator = null;
					getPartial2Evaluator();
				}
			} catch (HiveException e) {
				throw new RuntimeException(e);
			}
		}

		try {
			getPartial1Evaluator().iterate(acc, params);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}

	// TODO open this block after BLINK-17364558 resolved.
/*	public void merge(
			GenericUDAFEvaluator.AggregationBuffer acc,
			Iterable<GenericUDAFEvaluator.AggregationBuffer> it) {

		try {
			for (GenericUDAFEvaluator.AggregationBuffer agg : it) {

				getPartial2Evaluator().merge(
						acc,
						getPartial2Evaluator().terminatePartial(agg));
			}
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}*/

	public void resetAccumulator(GenericUDAFEvaluator.AggregationBuffer acc) {

		try {
			getPartial1Evaluator().reset(acc);
			getPartial2Evaluator().reset(acc);
			getFinalEvaluator().reset(acc);
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void open(FunctionContext context) throws Exception {

		this.resolver = newResolver();
		this.partial1Evaluator = null;
		this.partial2Evaluator = null;
		this.finalEvaluator = null;
	}

	private GenericUDAFEvaluator getFinalEvaluator() throws HiveException {

		if (finalEvaluatorByVoid) {
			// If it is not from the real finalEvaluator
			finalEvaluator = null;
		}
		if (null == finalEvaluator) {
			// If real params are null, use the one Void params.
			if (null == inputInspectors) {
				SimpleGenericUDAFParameterInfo paramInfo = getLazyVoidOneParam();
				finalEvaluator = resolver.getEvaluator(paramInfo);
				inputInspectors = paramInfo.getParameterObjectInspectors();
				finalEvaluatorByVoid = true;
			} else {
				SimpleGenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(
						inputInspectors, false, false, false);
				finalEvaluator = resolver.getEvaluator(paramInfo);
				finalEvaluatorByVoid = false;
			}
			returnInspector = finalEvaluator.init(
					GenericUDAFEvaluator.Mode.FINAL,
					inputInspectors);
		}
		return this.finalEvaluator;
	}

	private GenericUDAFEvaluator getPartial1Evaluator() throws HiveException {

		if (partial1EvaluatorByVoid) {
			partial1Evaluator = null;
		}
		if (null == partial1Evaluator) {
			if (null == inputInspectors) {
				SimpleGenericUDAFParameterInfo paramInfo = getLazyVoidOneParam();
				partial1Evaluator = resolver.getEvaluator(paramInfo);
				inputInspectors = paramInfo.getParameterObjectInspectors();
				partial1EvaluatorByVoid = true;
			} else {
				SimpleGenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(
						inputInspectors, false, false, false);
				partial1Evaluator = resolver.getEvaluator(paramInfo);
				partial1EvaluatorByVoid = false;
			}
			partialResultInspector = partial1Evaluator.init(
					GenericUDAFEvaluator.Mode.PARTIAL1,
					inputInspectors);
		}
		return this.partial1Evaluator;
	}

	private GenericUDAFEvaluator getPartial2Evaluator() throws HiveException {

		if (partial2EvaluatorByVoid) {
			partial2Evaluator = null;
		}
		if (null == partial2Evaluator) {
			if (null == partialResultInspector) {
				SimpleGenericUDAFParameterInfo parameterInfo = getLazyVoidOneParam();
				partial2Evaluator = resolver.getEvaluator(parameterInfo);
				partialResultInspector = parameterInfo.getParameterObjectInspectors()[0];
				partial2EvaluatorByVoid = true;
			} else {
				SimpleGenericUDAFParameterInfo paramInfo = new SimpleGenericUDAFParameterInfo(
						inputInspectors, false, false, false);
				partial2Evaluator = resolver.getEvaluator(paramInfo);
				partial2EvaluatorByVoid = false;
			}
			// The input of Partial 2 is th Output of the Partial 1.
			ObjectInspector[] partial1Types = new ObjectInspector[1];
			partial1Types[0] = partialResultInspector;
			partial2Evaluator.init(
					GenericUDAFEvaluator.Mode.PARTIAL2,
					partial1Types);
		}
		return this.partial2Evaluator;
	}

	private SimpleGenericUDAFParameterInfo getLazyVoidOneParam() {

		ObjectInspector[] objectInspectors = new ObjectInspector[1];
		objectInspectors[0] = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
				PrimitiveObjectInspector.PrimitiveCategory.LONG);
		return new SimpleGenericUDAFParameterInfo(
				objectInspectors, false, false, false);
	}

	@Override
	public GenericUDAFEvaluator.AggregationBuffer createAccumulator() {

		try {
			if (null == resolver) {
				// This method may be called at the client side.
				resolver = newResolver();
			}
			return getPartial1Evaluator().getNewAggregationBuffer();
		} catch (HiveException | IllegalAccessException | InstantiationException | ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public BaseRow getValue(GenericUDAFEvaluator.AggregationBuffer accumulator) {

		try {
			Object result = getFinalEvaluator().terminate(accumulator);
			Object flinkResult = HiveInspectors.unwrap(result, returnInspector);
			GenericRow value = new GenericRow(1);
			value.update(0, flinkResult);
			return value;
		} catch (HiveException e) {
			throw new RuntimeException(e);
		}
	}
}
