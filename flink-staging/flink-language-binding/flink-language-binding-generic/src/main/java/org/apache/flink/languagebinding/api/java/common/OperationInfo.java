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
package org.apache.flink.languagebinding.api.java.common;

import java.io.IOException;
import java.util.Arrays;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple;
import static org.apache.flink.api.java.typeutils.TypeExtractor.getForObject;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.languagebinding.api.java.common.PlanBinder.Operation;
import static org.apache.flink.languagebinding.api.java.common.PlanBinder.normalizeKeys;
import static org.apache.flink.languagebinding.api.java.common.PlanBinder.toIntArray;
import org.apache.flink.languagebinding.api.java.common.streaming.Receiver;

/**
 * Container for all generic information related to operations. This class contains the absolute minimum fields that are
 * required for all operations. This class should be extended to contain any additional fields required on a
 * per-language basis.
 */
public class OperationInfo {
	public int parentID; //DataSet that an operation is applied on
	public int otherID; //secondary DataSet
	public int setID; //ID for new DataSet
	public String[] keys;
	public String[] keys1; //join/cogroup keys
	public String[] keys2; //join/cogroup keys
	public TypeInformation<?> types; //typeinformation about output type
	public AggregationEntry[] aggregates;
	public ProjectionEntry[] projections; //projectFirst/projectSecond
	public Object[] values;
	public int count;
	public int field;
	public int[] fields;
	public Order order;
	public String path;
	public String fieldDelimiter;
	public String lineDelimiter;
	public long from;
	public long to;
	public WriteMode writeMode;
	public boolean toError;
	public String name;

	public OperationInfo() {
	}

	public OperationInfo(Receiver receiver, Operation identifier) throws IOException {
		Object tmpType;
		switch (identifier) {
			case SOURCE_CSV:
				setID = (Integer) receiver.getRecord(true);
				path = (String) receiver.getRecord();
				fieldDelimiter = (String) receiver.getRecord();
				lineDelimiter = (String) receiver.getRecord();
				tmpType = (Tuple) receiver.getRecord();
				types = tmpType == null ? null : getForObject(tmpType);
				return;
			case SOURCE_TEXT:
				setID = (Integer) receiver.getRecord(true);
				path = (String) receiver.getRecord();
				return;
			case SOURCE_VALUE:
				setID = (Integer) receiver.getRecord(true);
				int valueCount = (Integer) receiver.getRecord(true);
				values = new Object[valueCount];
				for (int x = 0; x < valueCount; x++) {
					values[x] = receiver.getRecord();
				}
				return;
			case SOURCE_SEQ:
				setID = (Integer) receiver.getRecord(true);
				from = (Long) receiver.getRecord();
				to = (Long) receiver.getRecord();
				return;
			case SINK_CSV:
				parentID = (Integer) receiver.getRecord(true);
				path = (String) receiver.getRecord();
				fieldDelimiter = (String) receiver.getRecord();
				lineDelimiter = (String) receiver.getRecord();
				writeMode = ((Integer) receiver.getRecord(true)) == 1
						? WriteMode.OVERWRITE
						: WriteMode.NO_OVERWRITE;
				return;
			case SINK_TEXT:
				parentID = (Integer) receiver.getRecord(true);
				path = (String) receiver.getRecord();
				writeMode = ((Integer) receiver.getRecord(true)) == 1
						? WriteMode.OVERWRITE
						: WriteMode.NO_OVERWRITE;
				return;
			case SINK_PRINT:
				parentID = (Integer) receiver.getRecord(true);
				toError = (Boolean) receiver.getRecord();
				return;
			case BROADCAST:
				parentID = (Integer) receiver.getRecord(true);
				otherID = (Integer) receiver.getRecord(true);
				name = (String) receiver.getRecord();
				return;
		}
		setID = (Integer) receiver.getRecord(true);
		parentID = (Integer) receiver.getRecord(true);
		switch (identifier) {
			case AGGREGATE:
				count = (Integer) receiver.getRecord(true);
				aggregates = new AggregationEntry[count];
				for (int x = 0; x < count; x++) {
					int encodedAgg = (Integer) receiver.getRecord(true);
					int field = (Integer) receiver.getRecord(true);
					aggregates[x] = new AggregationEntry(encodedAgg, field);
				}
				return;
			case FIRST:
				count = (Integer) receiver.getRecord(true);
				return;
			case DISTINCT:
			case GROUPBY:
			case PARTITION_HASH:
				keys = normalizeKeys(receiver.getRecord(true));
				return;
			case PROJECTION:
				fields = toIntArray(receiver.getRecord(true));
				return;
			case REBALANCE:
				return;
			case SORT:
				field = (Integer) receiver.getRecord(true);
				int encodedOrder = (Integer) receiver.getRecord(true);
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
				return;
			case UNION:
				otherID = (Integer) receiver.getRecord(true);
				return;
		}
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
		sb.append("Aggregates: ").append(Arrays.toString(aggregates)).append("\n");
		sb.append("Projections: ").append(Arrays.toString(projections)).append("\n");
		sb.append("Count: ").append(count).append("\n");
		sb.append("Field: ").append(field).append("\n");
		sb.append("Order: ").append(order.toString()).append("\n");
		sb.append("Path: ").append(path).append("\n");
		sb.append("FieldDelimiter: ").append(fieldDelimiter).append("\n");
		sb.append("LineDelimiter: ").append(lineDelimiter).append("\n");
		sb.append("From: ").append(from).append("\n");
		sb.append("To: ").append(to).append("\n");
		sb.append("WriteMode: ").append(writeMode).append("\n");
		sb.append("toError: ").append(toError).append("\n");
		return sb.toString();
	}

	public static class AggregationEntry {
		public Aggregations agg;
		public int field;

		public AggregationEntry(int encodedAgg, int field) {
			switch (encodedAgg) {
				case 0:
					agg = Aggregations.MAX;
					break;
				case 1:
					agg = Aggregations.MIN;
					break;
				case 2:
					agg = Aggregations.SUM;
					break;
			}
			this.field = field;
		}

		@Override
		public String toString() {
			return agg.toString() + " - " + field;
		}
	}

	public static class ProjectionEntry {
		public ProjectionSide side;
		public int[] keys;

		public ProjectionEntry(ProjectionSide side, int[] keys) {
			this.side = side;
			this.keys = keys;
		}

		@Override
		public String toString() {
			return side + " - " + Arrays.toString(keys);
		}
	}

	public enum ProjectionSide {
		FIRST,
		SECOND
	}

	public enum DatasizeHint {
		NONE,
		TINY,
		HUGE
	}
}
