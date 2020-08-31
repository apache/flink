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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.hive.util.HiveFunctionUtil;
import org.apache.flink.table.runtime.types.ClassLogicalTypeConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategy;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Abstract class to provide more information for Hive {@link UDF} and {@link GenericUDF} functions.
 */
@Internal
public abstract class HiveScalarFunction<UDFType> extends ScalarFunction {

	protected final HiveFunctionWrapper<UDFType> hiveFunctionWrapper;

	protected Object[] constantArguments;
	protected DataType[] argTypes;

	protected transient UDFType function;
	protected transient ObjectInspector returnInspector;

	private transient boolean isArgsSingleArray;

	HiveScalarFunction(HiveFunctionWrapper<UDFType> hiveFunctionWrapper) {
		this.hiveFunctionWrapper = hiveFunctionWrapper;
	}

	@Override
	public boolean isDeterministic() {
		try {
			org.apache.hadoop.hive.ql.udf.UDFType udfType =
				hiveFunctionWrapper.getUDFClass()
					.getAnnotation(org.apache.hadoop.hive.ql.udf.UDFType.class);

			return udfType != null && udfType.deterministic() && !udfType.stateful();
		} catch (ClassNotFoundException e) {
			throw new FlinkHiveUDFException(e);
		}
	}

	@Override
	public void open(FunctionContext context) {
		openInternal();

		isArgsSingleArray = HiveFunctionUtil.isSingleBoxedArray(argTypes);
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		TypeInference.Builder builder = TypeInference.newBuilder();
		builder.inputTypeStrategy(new HiveUDFInputStrategy());
		builder.outputTypeStrategy(new HiveUDFOutputStrategy());
		return builder.build();
	}

	/**
	 * See {@link ScalarFunction#open(FunctionContext)}.
	 */
	protected abstract void openInternal();

	public Object eval(Object... args) {

		// When the parameter is (Integer, Array[Double]), Flink calls udf.eval(Integer, Array[Double]), which is not a problem.
		// But when the parameter is an single array, Flink calls udf.eval(Array[Double]),
		// at this point java's var-args will cast Array[Double] to Array[Object] and let it be
		// Object... args, So we need wrap it.
		if (isArgsSingleArray) {
			args = new Object[] {args};
		}

		return evalInternal(args);
	}

	/**
	 * Evaluation logical, args will be wrapped when is a single array.
	 */
	protected abstract Object evalInternal(Object[] args);

	private void setArguments(CallContext callContext) {
		DataType[] inputTypes = callContext.getArgumentDataTypes().toArray(new DataType[0]);
		Object[] constantArgs = new Object[inputTypes.length];
		for (int i = 0; i < constantArgs.length; i++) {
			if (callContext.isArgumentLiteral(i)) {
				constantArgs[i] = callContext.getArgumentValue(
						i, ClassLogicalTypeConverter.getDefaultExternalClassForType(inputTypes[i].getLogicalType()))
						.orElse(null);
			}
		}
		this.constantArguments = constantArgs;
		this.argTypes = inputTypes;
	}

	/**
	 * Infer return type of this function call.
	 */
	protected abstract DataType inferReturnType() throws UDFArgumentException;

	private class HiveUDFOutputStrategy implements TypeStrategy {

		@Override
		public Optional<DataType> inferType(CallContext callContext) {
			setArguments(callContext);
			try {
				return Optional.of(inferReturnType());
			} catch (UDFArgumentException e) {
				throw new FlinkHiveUDFException(e);
			}
		}
	}

	private class HiveUDFInputStrategy implements InputTypeStrategy {

		@Override
		public ArgumentCount getArgumentCount() {
			return ConstantArgumentCount.any();
		}

		@Override
		public Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure) {
			setArguments(callContext);
			try {
				inferReturnType();
			} catch (UDFArgumentException e) {
				if (throwOnFailure) {
					throw new ValidationException(
							String.format("Cannot find a suitable Hive function from %s for the input arguments",
									hiveFunctionWrapper.getClassName()), e);
				} else {
					return Optional.empty();
				}
			}
			return Optional.of(callContext.getArgumentDataTypes());
		}

		@Override
		public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
			return Collections.singletonList(Signature.of(Signature.Argument.of("*")));
		}
	}
}
