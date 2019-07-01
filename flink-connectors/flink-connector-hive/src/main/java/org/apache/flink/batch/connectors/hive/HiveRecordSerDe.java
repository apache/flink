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

import org.apache.flink.types.Row;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DateObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
		} else if (fieldObjectInspector.getCategory() == ObjectInspector.Category.STRUCT) {
			res = convertStruct(field, (StructObjectInspector) fieldObjectInspector);
		} else if (fieldObjectInspector.getCategory() == ObjectInspector.Category.LIST) {
			res = convertList(field, (ListObjectInspector) fieldObjectInspector);
		} else if (fieldObjectInspector.getCategory() == ObjectInspector.Category.MAP) {
			res = convertMap(field, (MapObjectInspector) fieldObjectInspector);
		} else {
			throw new FlinkHiveException(new SerDeException(
					String.format("HiveRecordSerDe doesn't support category %s, type %s yet",
					fieldObjectInspector.getCategory(), fieldObjectInspector.getTypeName())));
		}
		return res;
	}

	private static Object convertStruct(Object field, StructObjectInspector soi) {
		List<? extends StructField> fields = soi.getAllStructFieldRefs();
		List<Object> list = soi.getStructFieldsDataAsList(field);

		if (list == null) {
			return null;
		}
		Row row = new Row(list.size());
		for (int i = 0; i < fields.size(); i++) {
			// Get the field objectInspector and the field object.
			ObjectInspector foi = fields.get(i).getFieldObjectInspector();
			Object f = list.get(i);
			row.setField(i, obtainFlinkRowField(f, foi));
		}
		return row;
	}

	private static Object convertList(Object field, ListObjectInspector loi) {
		List fields = loi.getList(field);
		if (fields == null) {
			return null;
		}

		ObjectInspector elementOI = loi.getListElementObjectInspector();
		Object[] objects = new Object[fields.size()];
		for (int i = 0; i < fields.size(); i++) {
			objects[i] = obtainFlinkRowField(fields.get(i), elementOI);
		}
		return objects;
	}

	private static Object convertMap(Object field, MapObjectInspector moi) {
		ObjectInspector koi = moi.getMapKeyObjectInspector();
		ObjectInspector voi = moi.getMapValueObjectInspector();
		Map<Object, Object> m = new HashMap<>();

		Map<?, ?> readMap = moi.getMap(field);
		if (readMap == null) {
			return null;
		} else {
			for (Map.Entry<?, ?> entry : readMap.entrySet()) {
				m.put(obtainFlinkRowField(entry.getKey(), koi), obtainFlinkRowField(entry.getValue(), voi));
			}
		}
		return m;
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
