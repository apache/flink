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

import org.apache.flink.table.dataformat.DataFormatConverters;
import org.apache.flink.table.type.DecimalType;

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
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

import java.math.BigDecimal;
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
	public static Object serializeField(Object field, ObjectInspector fieldObjectInspector)
			throws SerDeException {
		Object res;
		if (fieldObjectInspector.getCategory() == ObjectInspector.Category.PRIMITIVE) {
			res = serializePrimitiveField(field, (PrimitiveObjectInspector) fieldObjectInspector);
		} else {
			throw new SerDeException(HiveRecordSerDe.class.toString()
									+ " does not know what to do with fields of unknown category: "
									+ fieldObjectInspector.getCategory() + " , type: " + fieldObjectInspector.getTypeName());
		}
		return res;
	}

	/**
	 * This method actually convert java objects of Hive's scalar data types to those of Flink's internal data types.
	 * @param field field value
	 * @param primitiveObjectInspector Hive's primitive object inspector for the field
	 * @return the java objects conforming to Flink's internal data types.
	 *
	 * TODO: Comparing to original HCatRecordSerDe.java, we may need add more type converter according to conf.
	 */
	private static Object serializePrimitiveField(Object field, PrimitiveObjectInspector primitiveObjectInspector) {
		if (field == null) {
			return null;
		}

		switch(primitiveObjectInspector.getPrimitiveCategory()) {
			case DECIMAL:
				DecimalTypeInfo decimalTypeInfo = (DecimalTypeInfo) primitiveObjectInspector.getTypeInfo();
				HiveDecimalObjectInspector decimalOI = (HiveDecimalObjectInspector) primitiveObjectInspector;
				BigDecimal bigDecimal = decimalOI.getPrimitiveJavaObject(field).bigDecimalValue();
				DecimalType decimalType = new DecimalType(decimalTypeInfo.precision(), decimalTypeInfo.scale());
				return new DataFormatConverters.BigDecimalConverter(decimalType.precision(), decimalType.scale()).toInternal(bigDecimal);
			case TIMESTAMP:
				Timestamp ts = ((TimestampObjectInspector) primitiveObjectInspector).getPrimitiveJavaObject(field);
				return DataFormatConverters.TimestampConverter.INSTANCE.toInternal(ts);
			case DATE:
				int days = ((DateObjectInspector) primitiveObjectInspector).getPrimitiveWritableObject(field).getDays();
				return days;
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
