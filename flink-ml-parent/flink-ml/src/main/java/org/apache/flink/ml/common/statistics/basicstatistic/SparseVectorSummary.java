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

package org.apache.flink.ml.common.statistics.basicstatistic;

import org.apache.flink.ml.common.matrix.DenseVector;
import org.apache.flink.ml.common.matrix.SparseVector;
import org.apache.flink.ml.common.matrix.Vector;
import org.apache.flink.ml.common.statistics.StatisticsUtil;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * It is summarizer result of sparse vector.
 * You can get vectorSize, mean, variance, and other statistics from this class,
 *  and It will return sparse vector when get statistics.
 */
public class SparseVectorSummary extends BaseVectorSummary {

	protected int colNum = -1;

	protected Map <Integer, VectorStatCol> cols = new HashMap();

	public SparseVectorSummary() {
	}

	@Override
	public SparseVectorSummary clone() {
		SparseVectorSummary vsrt = new SparseVectorSummary();
		vsrt.count = count;
		vsrt.colNum = colNum;
		vsrt.cols = new HashMap();
		for (Map.Entry <Integer, VectorStatCol> entry : cols.entrySet()) {
			vsrt.cols.put(entry.getKey(), entry.getValue());
		}

		return vsrt;
	}

	@Override
	public String toString() {
		String[] outColNames = new String[] {"id", "count",
			"sum", "mean", "variance", "standardDeviation", "min", "max", "normL1", "normL2"};

		List <Row> data = new ArrayList();

		for (int i = 0; i < vectorSize(); i++) {
			Row row = new Row(outColNames.length);

			row.setField(0, i);
			row.setField(1, count);
			row.setField(2, sum(i));
			row.setField(3, mean(i));
			row.setField(4, variance(i));
			row.setField(5, standardDeviation(i));
			row.setField(6, min(i));
			row.setField(7, max(i));
			row.setField(8, normL1(i));
			row.setField(9, normL2(i));

			data.add(row);
		}

		return StatisticsUtil.toString(outColNames, data);
	}

	@Override
	public int vectorSize() {
		int maxIndx = -1;
		Integer[] indics = cols.keySet().toArray(new Integer[0]);
		for (int i = 0; i < indics.length; i++) {
			if (maxIndx < indics[i]) {
				maxIndx = indics[i];
			}
		}
		colNum = Math.max(colNum, maxIndx + 1);
		return colNum;
	}

	@Override
	public Vector sum() {
		int[] indices = getIndices();
		double[] data = new double[indices.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = cols.get(indices[i]).sum;
		}

		return new SparseVector(vectorSize(), indices, data);
	}

	@Override
	public Vector mean() {
		int[] indices = getIndices();
		double[] data = new double[indices.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = cols.get(indices[i]).mean(count);
		}

		return new SparseVector(vectorSize(), indices, data);
	}

	@Override
	public Vector variance() {
		int[] indices = getIndices();
		double[] data = new double[indices.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = cols.get(indices[i]).variance(count);
		}

		return new SparseVector(vectorSize(), indices, data);
	}

	@Override
	public Vector standardDeviation() {
		int[] indices = getIndices();
		double[] data = new double[indices.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = cols.get(indices[i]).standardDeviation(count);
		}

		return new SparseVector(vectorSize(), indices, data);
	}

	@Override
	public Vector min() {
		int[] indices = getIndices();
		double[] data = new double[indices.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = cols.get(indices[i]).min;
		}

		return new SparseVector(vectorSize(), indices, data);
	}

	@Override
	public Vector max() {
		int[] indices = getIndices();
		double[] data = new double[indices.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = cols.get(indices[i]).max;
		}

		return new SparseVector(vectorSize(), indices, data);
	}

	@Override
	public Vector normL1() {
		int[] indices = getIndices();
		double[] data = new double[indices.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = cols.get(indices[i]).normL1;
		}

		return new SparseVector(vectorSize(), indices, data);
	}

	@Override
	public Vector normL2() {
		int[] indices = getIndices();
		double[] data = new double[indices.length];
		for (int i = 0; i < data.length; i++) {
			data[i] = Math.sqrt(cols.get(indices[i]).sum2);
		}
		return new SparseVector(vectorSize(), indices, data);
	}

	public Vector numNonZero() {
		int colNum = vectorSize();
		System.out.println("vectorSize: " + colNum);
		double[] sum = new double[colNum];

		Iterator <Map.Entry <Integer, VectorStatCol>> entries = cols.entrySet().iterator();
		while (entries.hasNext()) {
			Map.Entry <Integer, VectorStatCol> entry = entries.next();
			sum[entry.getKey()] = entry.getValue().numNonZero;
		}
		return new DenseVector(sum);
	}

	public double numNonZero(int idx) {
		return numNonZero().get(idx);
	}

	private int[] getIndices() {
		Integer[] idxs = cols.keySet().toArray(new Integer[0]);
		int[] out = new int[idxs.length];
		for (int i = 0; i < out.length; i++) {
			out[i] = idxs[i];
		}
		return out;
	}

}
