/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.model;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.api.misc.param.Params;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * A utility class for converting model data to a collection of rows.
 */
class ModelConverterUtils {

	/**
	 * The size of a string segment. When serializing model data to a table,
	 * the string data will be sliced to segments with size no larger than "SEGMENT_SIZE".
	 */
	static final int SEGMENT_SIZE = 32 * 1024;

	/**
	 * Maximum number of slices a string can split to.
	 */
	static final long MAX_NUM_SLICES = 1024L * 1024L;

	/**
	 * Append model meta data to the collection of rows.
	 *
	 * @param meta      The model meta data.
	 * @param collector The collector of model rows.
	 * @param numFields Number of fields of the model table.
	 */
	static void appendMetaRow(Params meta, Collector<Row> collector, final int numFields) {
		if (meta != null) {
			appendStringData(meta.toJson(), collector, numFields, 0);
		}
	}

	/**
	 * Append a list of strings to the collection of rows.
	 *
	 * <p>Each of these string will be sliced to segments of size "SEGMENT_SIZE".
	 *
	 * @param data      The model data serialized to a list of strings.
	 * @param collector The collector of model rows.
	 * @param numFields Number of fields of the model table.
	 */
	static void appendDataRows(Iterable<String> data, Collector<Row> collector, final int numFields) {
		if (data != null) {
			int index = 0;
			for (String s : data) {
				appendStringData(s, collector, numFields, index + 1);
				index++;
			}
		}
	}

	/**
	 * Append a list of additional data to the collection of rows.
	 *
	 * @param auxData   The additional model data.
	 * @param collector The collector of model rows.
	 * @param numFields Number of fields of the model table.
	 * @param <T>       The type of additional data.
	 */
	static <T> void appendAuxiliaryData(Iterable<T> auxData, Collector<Row> collector, final int numFields) {
		if (auxData == null) {
			return;
		}

		final int numAdditionalFields = numFields - 2;
		int sliceIndex = 0;

		for (T data : auxData) {
			int stringIndex = Integer.MAX_VALUE;
			long modelId = getModelId(stringIndex, sliceIndex);
			Row row = new Row(numFields);
			row.setField(0, modelId);
			if (data instanceof Row) {
				Row r = (Row) data;
				for (int j = 0; j < numAdditionalFields; j++) {
					row.setField(2 + j, r.getField(j));
				}
			} else {
				row.setField(2, data);
			}
			collector.collect(row);
			sliceIndex++;
		}
	}

	/**
	 * Extract from a collection of rows the model meta and model data.
	 *
	 * @param rows Model rows.
	 * @return A tuple of model meta and serialized model data.
	 */
	static Tuple2<Params, Iterable<String>> extractModelMetaAndData(List<Row> rows) {
		Integer[] order = orderModelRows(rows);

		// extract meta
		List<String> metaSegments = new ArrayList<>();
		for (int i = 0; i < order.length; i++) {
			long id = (Long) rows.get(order[i]).getField(0);
			int currStringId = getStringIndex(id);
			if (currStringId == 0) {
				metaSegments.add((String) rows.get(order[i]).getField(1));
			} else {
				break;
			}
		}
		String metaStr = mergeString(metaSegments);

		return Tuple2.of(Params.fromJson(metaStr), new StringDataIterable(rows, order));
	}

	/**
	 * Extract the additional data from a collection of rows.
	 *
	 * @param rows    Model rows.
	 * @param isLabel Whether the additional data is label data.
	 * @param <T>     The type of additional data.
	 * @return The list of additional data.
	 */
	static <T> Iterable<T> extractAuxiliaryData(List<Row> rows, boolean isLabel) {
		Integer[] order = orderModelRows(rows);
		return new AuxiliaryDataIterable<T>(rows, order, isLabel);
	}

	private static class StringDataIterator implements Iterator<String> {
		List<Row> modelRows;
		Integer[] order;
		String curr;
		int listPos = 0;

		public StringDataIterator(List<Row> modelRows, Integer[] order) {
			this.modelRows = modelRows;
			this.order = order;
			if (getNextValue() == 0) { // skip meta data
				getNextValue();
			}
		}

		@Override
		public boolean hasNext() {
			return curr != null;
		}

		@Override
		public String next() {
			if (!hasNext()) {
				throw new RuntimeException("Iterator do not has next value.");
			}
			String ret = curr;
			getNextValue();
			return ret;
		}

		private int getNextValue() {
			List<String> segments = new ArrayList<>();
			int lastStringId = -1;
			while (true) {
				if (listPos >= order.length || modelRows.get(order[listPos]).getField(1) == null) {
					break;
				}
				long id = (Long) modelRows.get(order[listPos]).getField(0);
				String segment = (String) modelRows.get(order[listPos]).getField(1);

				int stringId = getStringIndex(id);
				if (lastStringId == -1) {
					lastStringId = stringId;
				}
				if (stringId != lastStringId) {
					break;
				} else {
					segments.add(segment);
					listPos++;
				}
			}
			if (segments.size() > 0) {
				curr = mergeString(segments);
				return lastStringId;
			} else {
				curr = null;
				return -1;
			}
		}
	}

	private static class StringDataIterable implements Iterable<String> {
		StringDataIterator iterator;

		public StringDataIterable(List<Row> modelRows, Integer[] order) {
			this.iterator = new StringDataIterator(modelRows, order);
		}

		@Override
		public Iterator<String> iterator() {
			return iterator;
		}
	}

	private static class AuxiliaryDataIterator<T> implements Iterator<T> {
		List<Row> modelRows;
		Integer[] order;
		boolean isLabel;
		boolean oldFormat;
		int listPos = 0;

		public AuxiliaryDataIterator(List<Row> modelRows, Integer[] order, boolean isLabel) {
			this.modelRows = modelRows;
			this.order = order;
			this.isLabel = isLabel;

			long lastId = (Long) modelRows.get(order[order.length - 1]).getField(0);
			oldFormat = getStringIndex(lastId) != Integer.MAX_VALUE;

			if (!oldFormat) {
				for (; listPos < order.length; listPos++) {
					long id = (Long) modelRows.get(order[listPos]).getField(0);
					if (getStringIndex(id) == Integer.MAX_VALUE) {
						break;
					}
				}
			}
		}

		@Override
		public boolean hasNext() {
			if (oldFormat) {
				return listPos < order.length && modelRows.get(order[listPos]).getField(2) != null;
			} else {
				return listPos < order.length;
			}
		}

		@Override
		public T next() {
			if (!hasNext()) {
				throw new RuntimeException("The iterator do not have next value.");
			}
			Object ret;
			Row modelRow = modelRows.get(order[listPos]);
			if (isLabel) {
				ret = modelRow.getField(2);
			} else {
				Row sub = new Row(modelRow.getArity() - 2);
				for (int j = 0; j < sub.getArity(); j++) {
					sub.setField(j, modelRow.getField(2 + j));
				}
				ret = sub;
			}
			listPos++;
			return (T) ret;
		}
	}

	private static class AuxiliaryDataIterable<T> implements Iterable<T> {
		AuxiliaryDataIterator<T> iterator;

		public AuxiliaryDataIterable(List<Row> modelRows, Integer[] order, boolean isLabel) {
			this.iterator = new AuxiliaryDataIterator<T>(modelRows, order, isLabel);
		}

		@Override
		public Iterator<T> iterator() {
			return iterator;
		}
	}

	private static void appendStringData(
		String data, Collector<Row> collector,
		final int numFields, int pos) {
		StringSlicer slicer = new StringSlicer(data, SEGMENT_SIZE);
		int i = 0;
		while (slicer.hasNextSegment()) {
			long modelId = getModelId(pos, i);
			Row row = new Row(numFields);
			row.setField(0, modelId);
			row.setField(1, slicer.nextSegment());
			collector.collect(row);
			i++;
		}
	}

	private static long getModelId(int stringIndex, int sliceIndex) {
		return MAX_NUM_SLICES * stringIndex + sliceIndex;
	}

	private static int getStringIndex(long modelId) {
		return (int) ((modelId) / MAX_NUM_SLICES);
	}

	private static Integer[] orderModelRows(List<Row> rows) {
		Integer[] order = new Integer[rows.size()];
		for (int i = 0; i < order.length; i++) {
			order[i] = i;
		}
		Arrays.sort(order, new Comparator<Integer>() {
			@Override
			public int compare(Integer o1, Integer o2) {
				return Long.compare((Long) rows.get(o1).getField(0), (Long) rows.get(o2).getField(0));
			}
		});
		return order;
	}

	private static class StringSlicer {
		private int segmentSize;
		private String str;
		private int pos;
		private int len;

		public StringSlicer(String str, int segmentSize) {
			this.segmentSize = segmentSize;
			this.str = str;
			this.pos = 0;
			this.len = str == null ? 0 : str.length();
		}

		public boolean hasNextSegment() {
			return pos < len;
		}

		public String nextSegment() {
			String segment = str.substring(pos, Math.min(pos + segmentSize, len));
			pos += segment.length();
			return segment;
		}
	}

	private static String mergeString(List<String> strings) {
		if (strings.size() == 1) { // this is the most cases.
			return strings.get(0);
		}
		StringBuilder sbd = new StringBuilder();
		strings.forEach(sbd::append);
		return sbd.toString();
	}
}
