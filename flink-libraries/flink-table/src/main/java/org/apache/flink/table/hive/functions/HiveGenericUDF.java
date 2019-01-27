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
import org.apache.flink.table.api.types.DataType;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.util.ArrayList;
import java.util.List;

/**
 * A Flink Scalar Function wrapper which wraps a Hive's GenericUDF.
 */
public class HiveGenericUDF extends ScalarFunction {

	private final HiveFunctionWrapper<GenericUDF> hiveFunctionWrapper;

	private transient GenericUDF function;
	private transient ObjectInspector returnInspector;
	private transient GenericUDF.DeferredObject[] deferredObjects;
	private transient boolean initialized = false;

	public HiveGenericUDF(HiveFunctionWrapper<GenericUDF> hiveFunctionWrapper) {
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
		ObjectInspector[] argumentInspectors =
				HiveInspectors.toInspectors(args, hiveFunctionWrapper.getConstants());
		returnInspector = function.initializeAndFoldConstants(argumentInspectors);
		deferredObjects = new GenericUDF.DeferredObject[args.length];
		for (int i = 0; i < deferredObjects.length; i++) {
			deferredObjects[i] = new DeferredObjectAdapter();
		}
		initialized = true;
	}

	public Object eval(Object... args) throws HiveException {
		if (!initialized) {
			// Initialize at the server side
			initialize(args);
		}
		int i = 0;
		while (i < args.length) {
			((DeferredObjectAdapter) deferredObjects[i]).set(args[i]);
			i++;
		}
		Object obj = function.evaluate(deferredObjects);
		return HiveInspectors.unwrap(obj, returnInspector);
	}

	@Override
	public DataType getResultType(Object[] arguments, Class[] argTypes) {
		try {
			if (null == function) {
				function = hiveFunctionWrapper.createFunction();
			}
			List<Boolean> contants = new ArrayList<>();
			for (Object argument : arguments) {
				if (argument == null) {
					contants.add(false);
				} else {
					contants.add(true);
				}
			}
			hiveFunctionWrapper.setConstants(contants);
			// Initialize at the client side
			initialize(arguments);
		} catch (UDFArgumentException | IllegalAccessException | ClassNotFoundException | InstantiationException e) {
			throw new RuntimeException(e);
		}
		return HiveInspectors.inspectorToDataType(returnInspector);
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
