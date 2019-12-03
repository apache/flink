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
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ScalarFunction implementation that calls Hive's {@link GenericUDF}.
 */
@Internal
public class HiveGenericUDF extends HiveScalarFunction<GenericUDF> {

	private static final Logger LOG = LoggerFactory.getLogger(HiveGenericUDF.class);

	private transient GenericUDF.DeferredObject[] deferredObjects;
	private HiveShim hiveShim;

	public HiveGenericUDF(HiveFunctionWrapper<GenericUDF> hiveFunctionWrapper, HiveShim hiveShim) {
		super(hiveFunctionWrapper);
		this.hiveShim = hiveShim;
		LOG.info("Creating HiveGenericUDF from '{}'", hiveFunctionWrapper.getClassName());
	}

	@Override
	public void openInternal() {

		LOG.info("Open HiveGenericUDF as {}", hiveFunctionWrapper.getClassName());

		function = hiveFunctionWrapper.createFunction();

		ObjectInspector[] argInspectors = HiveInspectors.toInspectors(hiveShim, constantArguments, argTypes);

		try {
			returnInspector = function.initializeAndFoldConstants(argInspectors);
		} catch (UDFArgumentException e) {
			throw new FlinkHiveUDFException(e);
		}

		deferredObjects = new GenericUDF.DeferredObject[argTypes.length];

		for (int i = 0; i < deferredObjects.length; i++) {
			deferredObjects[i] = new DeferredObjectAdapter(
				argInspectors[i],
				argTypes[i].getLogicalType(),
				hiveShim
			);
		}
	}

	@Override
	public Object evalInternal(Object[] args) {

		for (int i = 0; i < args.length; i++) {
			((DeferredObjectAdapter) deferredObjects[i]).set(args[i]);
		}

		try {
			return HiveInspectors.toFlinkObject(returnInspector, function.evaluate(deferredObjects), hiveShim);
		} catch (HiveException e) {
			throw new FlinkHiveUDFException(e);
		}
	}

	@Override
	public DataType getHiveResultType(Object[] constantArguments, DataType[] argTypes) {
		LOG.info("Getting result type of HiveGenericUDF from {}", hiveFunctionWrapper.getClassName());

		try {
			ObjectInspector[] argumentInspectors = HiveInspectors.toInspectors(hiveShim, constantArguments, argTypes);

			ObjectInspector resultObjectInspector =
				hiveFunctionWrapper.createFunction().initializeAndFoldConstants(argumentInspectors);

			return HiveTypeUtil.toFlinkType(
				TypeInfoUtils.getTypeInfoFromObjectInspector(resultObjectInspector));
		} catch (UDFArgumentException e) {
			throw new FlinkHiveUDFException(e);
		}
	}
}
