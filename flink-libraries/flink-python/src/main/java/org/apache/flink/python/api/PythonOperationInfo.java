/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.python.api.streaming.plan.PythonPlanStreamer;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForObject;

/**
 * Generic container for all information required to an operation to the DataSet API.
 */
public class PythonOperationInfo {
	public String identifier;
	public int parentID; //DataSet that an operation is applied on
	public int otherID; //secondary DataSet
	public int setID; //ID for new DataSet
	public String[] keys;
	public String[] keys1; //join/cogroup keys
	public String[] keys2; //join/cogroup keys
	public TypeInformation<?> types; //typeinformation about output type
	public Object[] values;
	public int count;
	public String field;
	public Order order;
	public String path;
	public String fieldDelimiter;
	public String lineDelimiter;
	public long frm;
	public long to;
	public WriteMode writeMode;
	public boolean toError;
	public String name;
	public boolean usesUDF;
	public int parallelism;
	public int envID;

	public PythonOperationInfo(PythonPlanStreamer streamer, int environmentID) throws IOException {
		identifier = (String) streamer.getRecord();
		parentID = (Integer) streamer.getRecord(true);
		otherID = (Integer) streamer.getRecord(true);
		field = "f0.f" + (Integer) streamer.getRecord(true);
		int encodedOrder = (Integer) streamer.getRecord(true);
		switch (encodedOrder) {
			case 0:
				order = Order.NONE;
				break;
			case 1:
				order = Order.ASCENDING;
				break;
			case 2:
				order = Order.DESCENDING;
				break;
			case 3:
				order = Order.ANY;
				break;
			default:
				order = Order.NONE;
				break;
		}
		keys = normalizeKeys(streamer.getRecord(true));
		keys1 = normalizeKeys(streamer.getRecord(true));
		keys2 = normalizeKeys(streamer.getRecord(true));
		Object tmpType = streamer.getRecord();
		types = tmpType == null ? null : getForObject(tmpType);
		usesUDF = (Boolean) streamer.getRecord();
		name = (String) streamer.getRecord();
		lineDelimiter = (String) streamer.getRecord();
		fieldDelimiter = (String) streamer.getRecord();
		writeMode = ((Integer) streamer.getRecord(true)) == 1
			? WriteMode.OVERWRITE
			: WriteMode.NO_OVERWRITE;
		path = (String) streamer.getRecord();
		frm = (Long) streamer.getRecord();
		to = (Long) streamer.getRecord();
		setID = (Integer) streamer.getRecord(true);
		toError = (Boolean) streamer.getRecord();
		count = (Integer) streamer.getRecord(true);
		int valueCount = (Integer) streamer.getRecord(true);
		values = new Object[valueCount];
		for (int x = 0; x < valueCount; x++) {
			values[x] = streamer.getRecord();
		}
		parallelism = (Integer) streamer.getRecord(true);

		envID = environmentID;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("SetID: ").append(setID).append("\n");
		sb.append("ParentID: ").append(parentID).append("\n");
		sb.append("OtherID: ").append(otherID).append("\n");
		sb.append("Name: ").append(name).append("\n");
		sb.append("Types: ").append(types).append("\n");
		sb.append("Keys1: ").append(Arrays.toString(keys1)).append("\n");
		sb.append("Keys2: ").append(Arrays.toString(keys2)).append("\n");
		sb.append("Keys: ").append(Arrays.toString(keys)).append("\n");
		sb.append("Count: ").append(count).append("\n");
		sb.append("Field: ").append(field).append("\n");
		sb.append("Order: ").append(order.toString()).append("\n");
		sb.append("Path: ").append(path).append("\n");
		sb.append("FieldDelimiter: ").append(fieldDelimiter).append("\n");
		sb.append("LineDelimiter: ").append(lineDelimiter).append("\n");
		sb.append("From: ").append(frm).append("\n");
		sb.append("To: ").append(to).append("\n");
		sb.append("WriteMode: ").append(writeMode).append("\n");
		sb.append("toError: ").append(toError).append("\n");
		return sb.toString();
	}

	enum DatasizeHint {
		NONE,
		TINY,
		HUGE
	}

	//====Utility=======================================================================================================
	private static String[] normalizeKeys(Object keys) {
		if (keys instanceof Tuple) {
			Tuple tupleKeys = (Tuple) keys;
			if (tupleKeys.getArity() == 0) {
				return new String[0];
			}
			if (tupleKeys.getField(0) instanceof Integer) {
				String[] stringKeys = new String[tupleKeys.getArity()];
				for (int x = 0; x < stringKeys.length; x++) {
					stringKeys[x] = "f0.f" + (Integer) tupleKeys.getField(x);
				}
				return stringKeys;
			}
			if (tupleKeys.getField(0) instanceof String) {
				return tupleToStringArray(tupleKeys);
			}
			throw new RuntimeException("Key argument contains field that is neither an int nor a String: " + tupleKeys);
		}
		if (keys instanceof int[]) {
			int[] intKeys = (int[]) keys;
			String[] stringKeys = new String[intKeys.length];
			for (int x = 0; x < stringKeys.length; x++) {
				stringKeys[x] = "f0.f" + intKeys[x];
			}
			return stringKeys;
		}
		throw new RuntimeException("Key argument is neither an int[] nor a Tuple: " + keys.toString());
	}

	private static String[] tupleToStringArray(Tuple tuple) {
		String[] keys = new String[tuple.getArity()];
		for (int y = 0; y < tuple.getArity(); y++) {
			keys[y] = (String) tuple.getField(y);
		}
		return keys;
	}
}
