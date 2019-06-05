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

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.IOException;

/**
 * Util class for accessing Hive tables.
 */
public class HiveTableUtil {

	private HiveTableUtil() {
	}

	/**
	 * Get Hive {@link ObjectInspector} for a Flink {@link TypeInformation}.
	 */
	public static ObjectInspector getObjectInspector(DataType flinkType) throws IOException {
		return getObjectInspector(HiveTypeUtil.toHiveTypeInfo(flinkType));
	}

	// TODO: reuse Hive's TypeInfoUtils?
	private static ObjectInspector getObjectInspector(TypeInfo type) throws IOException {
		switch (type.getCategory()) {

			case PRIMITIVE:
				PrimitiveTypeInfo primitiveType = (PrimitiveTypeInfo) type;
				return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(primitiveType);

			// TODO: support complex types
			default:
				throw new IOException("Unsupported Hive type category " + type.getCategory());
		}
	}
}
