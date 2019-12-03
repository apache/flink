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
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.conversion.HiveObjectConversion;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Adapter from Java Object to Hive DeferredObject.
 */
@Internal
public class DeferredObjectAdapter implements GenericUDF.DeferredObject {

	private Object object;
	private HiveObjectConversion conversion;

	public DeferredObjectAdapter(ObjectInspector inspector, LogicalType logicalType, HiveShim hiveShim) {
		conversion = HiveInspectors.getConversion(inspector, logicalType, hiveShim);
	}

	public void set(Object ob) {
		this.object = ob;
	}

	@Override
	public void prepare(int version) {
	}

	@Override
	public Object get() {
		return conversion.toHiveObject(object);
	}
}
