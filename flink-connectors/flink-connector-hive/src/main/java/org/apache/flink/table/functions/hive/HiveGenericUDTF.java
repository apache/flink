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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.functions.hive.conversion.IdentityConversion;
import org.apache.flink.table.functions.hive.util.HiveFunctionUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A TableFunction implementation that calls Hive's {@link GenericUDTF}.
 */
@Internal
public class HiveGenericUDTF extends TableFunction<Row> implements HiveFunction {
	private static final Logger LOG = LoggerFactory.getLogger(HiveGenericUDTF.class);

	private final HiveFunctionWrapper<GenericUDTF> hiveFunctionWrapper;

	private Object[] constantArguments;
	private DataType[] argTypes;

	private transient GenericUDTF function;
	private transient StructObjectInspector returnInspector;
	private transient boolean isArgsSingleArray;

	private transient boolean allIdentityConverter;
	private transient HiveObjectConversion[] conversions;

	public HiveGenericUDTF(HiveFunctionWrapper<GenericUDTF> hiveFunctionWrapper) {
		this.hiveFunctionWrapper = hiveFunctionWrapper;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		function = hiveFunctionWrapper.createFunction();

		function.setCollector(input -> {
			Row row = (Row) HiveInspectors.toFlinkObject(returnInspector, input);
			HiveGenericUDTF.this.collect(row);
		});

		ObjectInspector[] argumentInspectors = HiveInspectors.toInspectors(constantArguments, argTypes);
		returnInspector = function.initialize(argumentInspectors);

		isArgsSingleArray = HiveFunctionUtil.isSingleBoxedArray(argTypes);

		conversions = new HiveObjectConversion[argumentInspectors.length];
		for (int i = 0; i < argumentInspectors.length; i++) {
			conversions[i] = HiveInspectors.getConversion(argumentInspectors[i], argTypes[i].getLogicalType());
		}

		allIdentityConverter = Arrays.stream(conversions)
			.allMatch(conv -> conv instanceof IdentityConversion);
	}

	// Will only take effect after calling open()
	@VisibleForTesting
	protected final void setCollector(Collector collector) {
		function.setCollector(collector);
	}

	public void eval(Object... args) throws HiveException {

		// When the parameter is (Integer, Array[Double]), Flink calls udf.eval(Integer, Array[Double]), which is not a problem.
		// But when the parameter is an single array, Flink calls udf.eval(Array[Double]),
		// at this point java's var-args will cast Array[Double] to Array[Object] and let it be
		// Object... args, So we need wrap it.
		if (isArgsSingleArray) {
			args = new Object[] {args};
		}

		checkArgument(args.length == conversions.length);

		if (!allIdentityConverter) {
			for (int i = 0; i < args.length; i++) {
				args[i] = conversions[i].toHiveObject(args[i]);
			}
		}

		function.process(args);
	}

	@Override
	public void setArgumentTypesAndConstants(Object[] constantArguments, DataType[] argTypes) {
		this.constantArguments = constantArguments;
		this.argTypes = argTypes;
	}

	@Override
	public DataType getHiveResultType(Object[] constantArguments, DataType[] argTypes) {
		LOG.info("Getting result type of HiveGenericUDTF with {}", hiveFunctionWrapper.getClassName());

		try {
			ObjectInspector[] argumentInspectors = HiveInspectors.toInspectors(constantArguments, argTypes);
			return HiveTypeUtil.toFlinkType(
				hiveFunctionWrapper.createFunction().initialize(argumentInspectors));
		} catch (UDFArgumentException e) {
			throw new FlinkHiveUDFException(e);
		}
	}

	@Override
	public TypeInformation getResultType() {
		return LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(
			getHiveResultType(this.constantArguments, this.argTypes));
	}

	@Override
	public void close() throws Exception {
		function.close();
	}
}
