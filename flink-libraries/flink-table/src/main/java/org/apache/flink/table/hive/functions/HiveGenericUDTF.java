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
import org.apache.flink.table.api.functions.TableFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.dataformat.BaseRow;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.Collector;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A Flink Table Function wrapper which wraps a Hive Generic UDTF.
 */
public class HiveGenericUDTF extends TableFunction<BaseRow> {

	private final HiveFunctionWrapper<GenericUDTF> hiveFunctionWrapper;
	private final UDTFCollector collector = new UDTFCollector();

	private transient GenericUDTF function;
	private transient boolean initialized;
	private transient StructObjectInspector returnInspector;

	public HiveGenericUDTF(HiveFunctionWrapper<GenericUDTF> hiveFunctionWrapper) {
		this.hiveFunctionWrapper = hiveFunctionWrapper;
		this.initialized = false;
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		function = hiveFunctionWrapper.createFunction();
		function.setCollector(collector);
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
		returnInspector = function.initialize(argumentInspectors);
		collector.setReturnInspector(returnInspector);
		initialized = true;
	}

	public void eval(Object... args) throws HiveException {
		if (!initialized) {
			// Initialize at the server side
			initialize(args);
		}
		function.process(args);
		collector.collectRows(this);
	}

	@Override
	public DataType getResultType(Object[] arguments, Class[] argTypes) {
		try {
			if (null == function) {
				function = hiveFunctionWrapper.createFunction();
				function.setCollector(collector);
				collector.setReturnInspector(returnInspector);
			}
			List<Boolean> constants = new ArrayList<>();
			for (Object argument : arguments) {
				if (null == argument) {
					constants.add(false);
				} else {
					constants.add(true);
				}
			}
			hiveFunctionWrapper.setConstants(constants);
			// Initialize at the client side
			initialize(arguments);
		} catch (IllegalAccessException | InstantiationException | ClassNotFoundException | UDFArgumentException e) {
			throw new RuntimeException(e);
		}
		return HiveInspectors.inspectorToDataType(returnInspector);
	}

	/**
	 * A UDTF Collector with bridges between the collector of Hive and Flink.
	 */
	public static class UDTFCollector implements Collector {

		private List<BaseRow> baseRows = new ArrayList<>();
		private transient StructObjectInspector returnInspector;

		public void setReturnInspector(StructObjectInspector returnInspector) {
			this.returnInspector = returnInspector;
		}

		@Override
		public void collect(Object input) {
			BaseRow row = (BaseRow) HiveInspectors.unwrap(input, returnInspector);
			baseRows.add(row);
		}

		public void collectRows(TableFunction<BaseRow> tableFunction) {
			Iterator<BaseRow> i = baseRows.iterator();
			while (i.hasNext()) {
				tableFunction.collect(i.next());
				i.remove();
			}
		}
	}
}
