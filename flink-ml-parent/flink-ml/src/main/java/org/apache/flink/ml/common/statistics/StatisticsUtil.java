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

package org.apache.flink.ml.common.statistics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.ml.batchoperator.BatchOperator;
import org.apache.flink.ml.common.matrix.DenseVector;
import org.apache.flink.ml.common.matrix.Vector;
import org.apache.flink.ml.common.statistics.basicstatistic.BaseVectorSummarizer;
import org.apache.flink.ml.common.statistics.basicstatistic.BaseVectorSummary;
import org.apache.flink.ml.common.statistics.basicstatistic.TableSummarizer;
import org.apache.flink.ml.common.statistics.basicstatistic.TableSummarizerPartiiton;
import org.apache.flink.ml.common.statistics.basicstatistic.TableSummary;
import org.apache.flink.ml.common.statistics.basicstatistic.VectorSummarizerPartition;
import org.apache.flink.ml.common.utils.RowUtil;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.ml.common.utils.Types;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.List;

/**
 * Util for common statistical calculation.
 */
public class StatisticsUtil {

	public static boolean isString(String dataType) {
		return "string".equals(dataType.trim().toLowerCase()) ? true : false;
	}

	public static boolean isBoolean(String dataType) {
		return "boolean".equals(dataType.trim().toLowerCase()) ? true : false;
	}

	public static boolean isDatetime(String dataType) {
		return "datetime".equals(dataType.trim().toLowerCase()) ? true : false;
	}

	public static String toTypeString(TypeInformation dataType) {
		return dataType.getTypeClass().getSimpleName().toLowerCase();
	}

	public static boolean isNumber(String dataType) {
		if ("double".equals(dataType)
			|| "long".equals(dataType)
			|| "byte".equals(dataType)
			|| "int".equals(dataType)
			|| "float".equals(dataType)
			|| "short".equals(dataType)
			|| "integer".equals(dataType)) {
			return true;
		} else {
			return false;
		}
	}

	public static boolean isNumber(TypeInformation dataType) {
		if (Types.DOUBLE == dataType
			|| Types.LONG == dataType
			|| Types.BYTE == dataType
			|| Types.INT == dataType
			|| Types.FLOAT == dataType
			|| Types.SHORT == dataType
			|| Types.BIG_INT == dataType) {
			return true;
		} else {
			return false;
		}
	}

	public static void isNumber(TableSchema schema, String[] selectedColNames) {
		for (int i = 0; i < selectedColNames.length; i++) {
			if (!isNumber(schema.getFieldType(selectedColNames[i]).get())) {
				throw new RuntimeException(
					"col must be double or long." + schema.getFieldType(selectedColNames[i]).get());
			}
		}
	}

	public static double minMaxScaler(double val, double eMin, double eMax, double maxV, double minV) {
		double valOut;
		if (eMin != eMax) {
			valOut = (val - eMin) / (eMax - eMin) * (maxV - minV) + minV;
		} else {
			valOut = 0.5 * (maxV + minV);
		}
		return valOut;
	}

	/**
	 * convert data into vector and compute summary.
	 * it deal with table which only have number cols and not has missing value;
	 */
	public static Tuple2 <DataSet <Tuple2 <Vector, Row>>, DataSet <BaseVectorSummary>>
	summaryHelper(BatchOperator in, String[] selectedColNames, String vectorColName, String[] keepColNames) {
		if (keepColNames == null) {
			keepColNames = new String[] {};
		}

		checkSimpleStatParameter(in, selectedColNames, vectorColName);

		DataSet <Tuple2 <Vector, Row>> data = transformToVec(in, selectedColNames, vectorColName, keepColNames);

		DataSet <Vector> vectorDataSet = data
			.map(new MapFunction <Tuple2 <Vector, Row>, Vector>() {
				@Override
				public Vector map(Tuple2 <Vector, Row> row) throws Exception {
					return row.f0;
				}
			});

		DataSet <BaseVectorSummary> vsrt = summarizer(vectorDataSet);

		return new Tuple2(data, vsrt);
	}

	/**
	 * convert data into vector without keepColNames and compute summary.
	 * it deal with table which only have number cols and not has missing value;
	 */
	public static Tuple2 <DataSet <Vector>, DataSet <BaseVectorSummary>>
	summaryHelper(BatchOperator in, String[] selectedColNames, String vectorColName) {
		checkSimpleStatParameter(in, selectedColNames, vectorColName);

		DataSet <Vector> data = transformToVec(in, selectedColNames, vectorColName);

		DataSet <BaseVectorSummary> vsrt = summarizer(data);

		return new Tuple2(data, vsrt);
	}

	/**
	 * table stat.
	 */
	public static DataSet <TableSummary> summary(BatchOperator in, String[] selectedColNames) {
		if (selectedColNames == null || selectedColNames.length == 0) {
			throw new InvalidParameterException("selectedColNames must be set.");
		}

		in = in.select(selectedColNames);

		List <Integer> numberColIdxsList = new ArrayList <>();

		TypeInformation[] colTypes = in.getColTypes();
		for (int i = 0; i < colTypes.length; i++) {
			if (isNumber(colTypes[i])) {
				numberColIdxsList.add(i);
			}
		}

		int[] numberColIdx = new int[numberColIdxsList.size()];
		for (int i = 0; i < numberColIdx.length; i++) {
			numberColIdx[i] = numberColIdxsList.get(i);
		}

		DataSet <TableSummary> vsrt = summarizer(in.getDataSet(), numberColIdx, selectedColNames);

		return vsrt;
	}

	/**
	 * vector stat.
	 */
	public static DataSet <BaseVectorSummary> vectorSummary(BatchOperator in, String selectedColName) {
		if (TableUtil.findIndexFromName(in.getColNames(), selectedColName) < 0) {
			throw new RuntimeException(selectedColName + " is not exist.");
		}

		DataSet <Vector> data = transformToVec(in, null, selectedColName);
		DataSet <BaseVectorSummary> vsrt = summarizer(data, false)
			.map(new MapFunction <BaseVectorSummarizer, BaseVectorSummary>() {
				@Override
				public BaseVectorSummary map(BaseVectorSummarizer summarizer) throws Exception {
					return summarizer.toSummary();
				}
			});
		return vsrt;
	}

	/**
	 * table stat.
	 */
	public static DataSet <TableSummarizer> summarizer(BatchOperator in, String[] selectedColNames, boolean bCov) {
		if (selectedColNames == null || selectedColNames.length == 0) {
			throw new InvalidParameterException("selectedColNames must be set.");
		}

		in = in.select(selectedColNames);

		List <Integer> numberColIdxsList = new ArrayList <>();

		TypeInformation[] colTypes = in.getColTypes();
		for (int i = 0; i < colTypes.length; i++) {
			if (isNumber(colTypes[i])) {
				numberColIdxsList.add(i);
			}
		}

		int[] numberColIdx = new int[numberColIdxsList.size()];
		for (int i = 0; i < numberColIdx.length; i++) {
			numberColIdx[i] = numberColIdxsList.get(i);
		}

		DataSet <TableSummarizer> vsrt = summarizer(in.getDataSet(), bCov, numberColIdx, selectedColNames);

		return vsrt;
	}

	/**
	 * vector stat.
	 */
	public static DataSet <BaseVectorSummarizer> vectorSummarizer(
		BatchOperator in, String selectedColName, boolean bCov) {
		if (TableUtil.findIndexFromName(in.getColNames(), selectedColName) < 0) {
			throw new RuntimeException(selectedColName + " is not exist.");
		}

		DataSet <Vector> data = transformToVec(in, null, selectedColName);
		DataSet <BaseVectorSummarizer> vsrt = summarizer(data, bCov);

		return vsrt;
	}

	public static DataSet <Vector> transformToVec(
		BatchOperator in, String[] selectedColNames, String vectorColName) {
		DataSet <Vector> data;

		if (selectedColNames != null && selectedColNames.length != 0) {
			int[] selectedColIdxs = TableUtil.findIndexFromName(in.getColNames(), selectedColNames);
			data = in.getDataSet().map(new TransferToDenseVector2(selectedColIdxs));
		} else {
			int selectColIdx = TableUtil.findIndexFromName(in.getColNames(), vectorColName);
			data = in.getDataSet().map(new VectorTransferToDenseVector2(selectColIdx));
		}
		return data;
	}

	public static DataSet <Tuple2 <Vector, Row>> transformToVec(BatchOperator in,
																String[] selectedColNames,
																String vectorColName,
																String[] keepColNames) {
		DataSet <Tuple2 <Vector, Row>> data;

		int[] keepColIdxs = TableUtil.findIndexFromName(in.getColNames(), keepColNames);
		for (int i = 0; i < keepColIdxs.length; i++) {
			if (-1 == keepColIdxs[i]) {
				throw new RuntimeException("keepColNames is not exist. " + keepColNames[i]);
			}
		}

		if (selectedColNames != null && selectedColNames.length != 0) {
			int[] selectedColIdxs = TableUtil.findIndexFromName(in.getColNames(), selectedColNames);
			data = in.getDataSet().map(new TransferToDenseVector(selectedColIdxs, keepColIdxs));
		} else {
			int selectColIdx = TableUtil.findIndexFromName(in.getColNames(), vectorColName);
			data = in.getDataSet().map(new VectorTransferToDenseVector(selectColIdx, keepColIdxs));
		}
		return data;
	}

	public static DataSet <Row> transformToTable(
		BatchOperator in, String[] selectedColNames, String vectorColName) {
		int[] selectedColIdxs = null;
		if (selectedColNames != null && selectedColNames.length != 0) {
			selectedColIdxs = TableUtil.findIndexFromTable(in.getSchema(), selectedColNames);
		}

		int vectorColIdx = -1;
		if (vectorColName != null) {
			vectorColIdx = TableUtil.findIndexFromName(in.getColNames(), vectorColName);
		}

		TransferToVectorMap transfer = new TransferToVectorMap(selectedColIdxs, vectorColIdx);
		return in.getDataSet().map(transfer);
	}

	public static DataSet <Row> transformToTable(
		BatchOperator in, String[] selectedColNames, String vectorColName, String[] keepColNames) {
		int[] selectedColIdxs = null;
		if (selectedColNames != null && selectedColNames.length != 0) {
			selectedColIdxs = TableUtil.findIndexFromTable(in.getSchema(), selectedColNames);
		}

		int vectorColIdx = -1;
		if (vectorColName != null) {
			vectorColIdx = TableUtil.findIndexFromName(in.getColNames(), vectorColName);
		}

		int[] keepColIdxs = TableUtil.findIndexFromName(in.getColNames(), keepColNames);

		TransferToVectorMap2 transfer = new TransferToVectorMap2(selectedColIdxs, vectorColIdx, keepColIdxs);
		return in.getDataSet().map(transfer);
	}

	private static void checkSimpleStatParameter(BatchOperator in, String[] selectedColNames, String vectorColName) {
		if (selectedColNames != null && selectedColNames.length != 0 && vectorColName != null) {
			throw new RuntimeException("selectedColName and vectorColName must be set one only.");
		}

		if (selectedColNames != null && selectedColNames.length != 0) {
			int[] selectedColIdxs = TableUtil.findIndexFromName(in.getColNames(), selectedColNames);
			for (int i = 0; i < selectedColNames.length; i++) {
				if (-1 == selectedColIdxs[i]) {
					throw new RuntimeException("selectedColNames is not exist. " + selectedColNames[i]);
				}
			}
			for (int i = 0; i < selectedColNames.length; i++) {
				TypeInformation type = in.getSchema().getFieldType(selectedColIdxs[i]).get();
				if (!isNumber(type)) {
					throw new RuntimeException("type must be number.");
				}
			}
		} else {
			int selectColIdx = TableUtil.findIndexFromName(in.getColNames(), vectorColName);
			if (-1 == selectColIdx) {
				throw new RuntimeException("vectorColanme is not exist. " + vectorColName);
			}
		}
	}

	private static DataSet <BaseVectorSummarizer> summarizer(DataSet <Vector> data, boolean bCov) {
		return data
			.mapPartition(new VectorSummarizerPartition(bCov))
			.reduce(new ReduceFunction <BaseVectorSummarizer>() {
						@Override
						public BaseVectorSummarizer reduce(BaseVectorSummarizer left, BaseVectorSummarizer right)
							throws
							Exception {
							return (BaseVectorSummarizer) BaseVectorSummarizer.merge(left, right);
						}
					}
			);
	}

	private static DataSet <BaseVectorSummary> summarizer(DataSet <Vector> data) {
		return summarizer(data, false)
			.map(new MapFunction <BaseVectorSummarizer, BaseVectorSummary>() {
				@Override
				public BaseVectorSummary map(BaseVectorSummarizer summarizer) throws Exception {
					return summarizer.toSummary();
				}
			});
	}

	private static DataSet <TableSummary> summarizer(
		DataSet <Row> data, int[] numberIdxs, String[] selectedColNames) {
		return summarizer(data, false, numberIdxs, selectedColNames)
			.map(new MapFunction <TableSummarizer, TableSummary>() {
				@Override
				public TableSummary map(TableSummarizer summarizer) throws Exception {
					return summarizer.toSummary();
				}
			});
	}

	private static DataSet <TableSummarizer> summarizer(DataSet <Row> data, boolean bCov, int[] numberIdxs,
														String[] selectedColNames) {
		return data
			.mapPartition(new TableSummarizerPartiiton(bCov, numberIdxs, selectedColNames))
			.reduce(new ReduceFunction <TableSummarizer>() {
						@Override
						public TableSummarizer reduce(TableSummarizer left, TableSummarizer right) throws Exception {
							return (TableSummarizer) TableSummarizer.merge(left, right);
						}
					}
			);
	}

	public static String getJson(List <Row> rows) {
		if (rows.size() != 1) {
			throw new RuntimeException("row size must be 1.");
		}
		Row row = rows.get(0);
		if (row.getArity() != 1) {
			throw new RuntimeException("row arity must be 1.");
		}
		Object obj = row.getField(0);
		if (obj instanceof String) {
			return (String) obj;
		} else {
			throw new RuntimeException("row arity must be string.");
		}
	}

	public static String toString(String[] colNames, List <Row> data) {
		StringBuilder sbd = new StringBuilder();
		sbd.append(RowUtil.formatTitle(colNames));

		for (Row row : data) {
			sbd.append("\n")
				.append(formatRows(row));

		}

		return sbd.toString();
	}

	private static String formatRows(Row row) {
		StringBuilder sb = new StringBuilder();

		for (int i = 0; i < row.getArity(); ++i) {
			if (i > 0) {
				sb.append("|");
			}
			Object obj = row.getField(i);
			if (obj == null) {
				sb.append("null");
			} else {
				if (obj instanceof Double || obj instanceof Float) {
					double val = (double) obj;
					if (Double.isNaN(val)) {
						sb.append("NaN");
					} else {
						sb.append(round(val, 4));
					}
				} else {
					sb.append(obj);
				}

			}
		}

		return sb.toString();
	}

	protected static double round(double v, int scale) {
		if (scale < 0) {
			throw new IllegalArgumentException(
				"The scale must be a positive integer or zero");
		}
		BigDecimal b = new BigDecimal(Double.toString(v));
		BigDecimal one = new BigDecimal("1");
		return b.divide(one, scale, BigDecimal.ROUND_HALF_UP).doubleValue();
	}

	private static class TransferToDenseVector implements MapFunction <Row, Tuple2 <Vector, Row>> {
		private int[] featureIdxs;
		private int[] keepColIdxs;

		public TransferToDenseVector(int[] featureIdxs, int[] keepColIdxs) {
			this.featureIdxs = featureIdxs;
			this.keepColIdxs = keepColIdxs;
		}

		@Override
		public Tuple2 <Vector, Row> map(Row in) throws Exception {
			Row out = new Row(keepColIdxs.length);

			double[] data = new double[featureIdxs.length];
			for (int i = 0; i < featureIdxs.length; i++) {
				data[i] = ((Number) in.getField(this.featureIdxs[i])).doubleValue();
			}

			DenseVector dv = new DenseVector(data);
			for (int i = 0; i < this.keepColIdxs.length; ++i) {
				out.setField(i, in.getField(this.keepColIdxs[i]));
			}
			return new Tuple2 <>(dv, out);
		}
	}

	private static class TransferToDenseVector2 implements MapFunction <Row, Vector> {
		private int[] featureIdxs;

		public TransferToDenseVector2(int[] featureIdxs) {
			this.featureIdxs = featureIdxs;
		}

		@Override
		public Vector map(Row in) throws Exception {
			double[] data = new double[featureIdxs.length];
			for (int i = 0; i < featureIdxs.length; i++) {
				Object obj = in.getField(this.featureIdxs[i]);
				if (obj instanceof Number) {
					data[i] = ((Number) obj).doubleValue();
				}
			}

			return new DenseVector(data);
		}
	}

	private static class VectorTransferToDenseVector implements MapFunction <Row, Tuple2 <Vector, Row>> {
		private int selectColIdx;
		private int[] keepColIdxs;

		public VectorTransferToDenseVector(int selectColIdx, int[] keepColIdxs) {
			this.selectColIdx = selectColIdx;
			this.keepColIdxs = keepColIdxs;
		}

		@Override
		public Tuple2 <Vector, Row> map(Row in) throws Exception {
			Row out = new Row(keepColIdxs.length);

			Vector vec = Vector.deserialize((String) in.getField(selectColIdx));
			for (int i = 0; i < this.keepColIdxs.length; ++i) {
				out.setField(i, in.getField(this.keepColIdxs[i]));
			}
			return new Tuple2(vec, out);
		}
	}

	private static class VectorTransferToDenseVector2 implements MapFunction <Row, Vector> {
		private int selectColIdx;

		public VectorTransferToDenseVector2(int selectColIdx) {
			this.selectColIdx = selectColIdx;
		}

		@Override
		public Vector map(Row in) throws Exception {
			return Vector.deserialize((String) in.getField(selectColIdx));
		}
	}

	/**
	 * convert table or vector to double dense vector.
	 */
	static class TransferToVectorMap implements MapFunction <Row, Row> {

		private int vectorIdx;
		private int[] featureIdxs;

		public TransferToVectorMap(int[] featureIdxs, int vectorIdx) {
			this.vectorIdx = vectorIdx;
			this.featureIdxs = featureIdxs;
		}

		@Override
		public Row map(Row in) throws Exception {
			Row out = null;
			if (featureIdxs == null) {
				String str = (String) in.getField(vectorIdx);
				DenseVector feature = Vector.deserialize(str).toDenseVector();
				if (feature.getData() != null) {
					out = new Row(feature.size());
					for (int i = 0; i < feature.size(); i++) {
						out.setField(i, feature.get(i));
					}
				} else {
					out = new Row(0);
				}
			} else {
				out = new Row(featureIdxs.length);
				for (int i = 0; i < this.featureIdxs.length; ++i) {
					out.setField(i, ((Number) in.getField(this.featureIdxs[i])).doubleValue());
				}
			}
			return out;
		}
	}

	/**
	 * convert table or vector to double dense vector.
	 */
	static class TransferToVectorMap2 implements MapFunction <Row, Row> {

		private int vectorIdx;
		private int[] featureIdxs;
		private int[] keepColIdxs;

		public TransferToVectorMap2(int[] featureIdxs, int vectorIdx, int[] keepColIdxs) {
			this.vectorIdx = vectorIdx;
			this.featureIdxs = featureIdxs;
			this.keepColIdxs = keepColIdxs;
		}

		@Override
		public Row map(Row in) throws Exception {
			Row out = null;
			if (featureIdxs == null) {
				String str = (String) in.getField(vectorIdx);
				DenseVector feature = Vector.deserialize(str).toDenseVector();
				if (feature.getData() != null) {
					out = new Row(feature.size() + keepColIdxs.length);
					for (int i = 0; i < feature.size(); i++) {
						out.setField(i, feature.get(i));
					}
					for (int i = 0; i < keepColIdxs.length; i++) {
						out.setField(i + feature.size(), in.getField(keepColIdxs[i]));
					}
				} else {
					out = new Row(0 + keepColIdxs.length);
					for (int i = 0; i < keepColIdxs.length; i++) {
						out.setField(i, in.getField(keepColIdxs[i]));
					}
				}
			} else {
				out = new Row(featureIdxs.length + keepColIdxs.length);
				for (int i = 0; i < this.featureIdxs.length; ++i) {
					out.setField(i, ((Number) in.getField(this.featureIdxs[i])).doubleValue());
				}
				for (int i = 0; i < keepColIdxs.length; i++) {
					out.setField(i + featureIdxs.length, in.getField(keepColIdxs[i]));
				}
			}
			return out;
		}
	}

}
