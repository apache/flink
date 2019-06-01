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

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Class used to serialize to and from raw hdfs file type.
 * Highly inspired by HCatRecordSerDe (almost copied from this class)in hive-catalog-core.
 */
public class HiveRecordSerDe {

	/**
	 * Return underlying Java Object from an object-representation
	 * that is readable by a provided ObjectInspector.
	 */
	public static Object obtainFlinkRowField(Object field, ObjectInspector fieldObjectInspector) {
		Object res;
		if (fieldObjectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
			res = convertPrimitiveField(field, (PrimitiveObjectInspector) fieldObjectInspector);
		} else {
			throw new FlinkHiveException(new SerDeException(
					String.format("HiveRecordSerDe doesn't support category %s, type %s yet",
					fieldObjectInspector.getCategory(), fieldObjectInspector.getTypeName())));
		}
		return res;
	}

	/**
	 * This method actually convert java objects of Hive's scalar data types to those of Flink's internal data types.
	 *
	 * @param field field value
	 * @param primitiveObjectInspector Hive's primitive object inspector for the field
	 * @return the java object conforming to Flink's internal data types.
	 *
	 * TODO: Comparing to original HCatRecordSerDe.java, we may need add more type converter according to conf.
	 */
	private static Object convertPrimitiveField(Object field, PrimitiveObjectInspector primitiveObjectInspector) {
		if (field == null) {
			return null;
		}

		switch(primitiveObjectInspector.getPrimitiveCategory()) {
			case DECIMAL:
				HiveDecimalObjectInspector decimalOI = (HiveDecimalObjectInspector) primitiveObjectInspector;
				BigDecimal bigDecimal = decimalOI.getPrimitiveJavaObject(field).bigDecimalValue();
				return bigDecimal;
			case TIMESTAMP:
				Timestamp ts = ((TimestampObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(field);
				return ts;
			case DATE:
				Date date = ((DateObjectInspector) primitiveObjectInspector).getPrimitiveWritableObject(field).get();
				return date;
			case CHAR:
				HiveChar c = ((HiveCharObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(field);
				return c.getStrippedValue();
			case VARCHAR:
				HiveVarchar vc = ((HiveVarcharObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(field);
				return vc.getValue();
			default:
				return primitiveObjectInspector.getPrimitiveJavaObject(field);
		}
	}
}
