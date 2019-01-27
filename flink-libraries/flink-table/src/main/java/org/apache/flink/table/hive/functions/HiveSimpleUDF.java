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

import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.ScalarFunction;

import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * A Hive Simple UDF is a ScalarFunction which calls Hive's
 * org.apache.hadoop.hive.ql.exec.UDF.
 */
public class HiveSimpleUDF extends ScalarFunction {

	private final HiveFunctionWrapper<UDF> hiveFunctionWrapper;

	private transient UDF function;
	private transient Method method;
	private transient List<TypeInfo> typeInfos;
	private transient ObjectInspector[] objectInspectors;
	private transient GenericUDFUtils.ConversionHelper conversionHelper;
	private transient boolean initialized = false;

	public HiveSimpleUDF(HiveFunctionWrapper<UDF> hiveFunctionWrapper) {
		this.hiveFunctionWrapper = hiveFunctionWrapper;
		this.initialized = false;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		function = hiveFunctionWrapper.createFunction();
		initialized = false;
	}

	@Override
	public void close() {
		// Users may use the instance later.
		initialized = false;
	}

	private void initialize(Object... args) throws UDFArgumentException {
		typeInfos = new ArrayList<>();
		for (Object arg : args) {
			typeInfos.add(
					TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(arg.getClass()));
		}
		method = function.getResolver().getEvalMethod(typeInfos);

		objectInspectors = new ObjectInspector[typeInfos.size()];
		for (int i = 0; i < args.length; i++) {
			objectInspectors[i] = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
					(PrimitiveTypeInfo) typeInfos.get(i));
		}
		conversionHelper = new GenericUDFUtils.ConversionHelper(method, objectInspectors);
		initialized = true;
	}

	public Object eval(Object... args) throws HiveException {
		if (!initialized) {
			initialize(args);
		}
		Object[] convertedArgs;
		try {
			convertedArgs = conversionHelper.convertIfNecessary(args);
		} catch (ClassCastException e) {
			// If a udf is called many times in the same SQL with different types.
			// It will be initialized many times with low efficiency.
			initialize(args);
			convertedArgs = conversionHelper.convertIfNecessary(args);
		}
		return FunctionRegistry.invoke(method, function, convertedArgs);
	}

	@Override
	public boolean isDeterministic() {
		try {
			UDFType udfType = hiveFunctionWrapper.getUDFClass().getAnnotation(UDFType.class);
			return udfType != null && udfType.deterministic() && !udfType.stateful();
		} catch (ClassNotFoundException e) {
			throw new RuntimeException(e);
		}
	}
}
