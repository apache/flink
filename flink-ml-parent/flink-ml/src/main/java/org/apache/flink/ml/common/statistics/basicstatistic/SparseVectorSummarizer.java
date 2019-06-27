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

import org.apache.flink.ml.common.matrix.DenseMatrix;
import org.apache.flink.ml.common.matrix.DenseVector;
import org.apache.flink.ml.common.matrix.SparseVector;
import org.apache.flink.ml.common.matrix.Vector;

import java.util.HashMap;
import java.util.Map;

/**
 * It is summarizer of sparse vector.
 * It uses Map to store median result.
 */
public class SparseVectorSummarizer extends BaseVectorSummarizer {

	protected int colNum = -1;

	protected Map <Integer, VectorStatCol> cols = new HashMap();

	public SparseVectorSummarizer() {
		this.bCov = false;
	}

	public SparseVectorSummarizer(boolean bCov) {
		this.bCov = bCov;
	}

	@Override
	public BaseVectorSummarizer visit(Vector vec) {
		if (vec instanceof SparseVector) {
			SparseVector sv = (SparseVector) vec;
			count++;

			this.colNum = Math.max(this.colNum, sv.n);

			if (sv.nnz != 0) {
				/**
				 * max index + 1 for size.
				 */
				int size = sv.indices[sv.indices.length - 1] + 1;
				for (int i = 0; i < sv.indices.length; i++) {
					int indice = sv.indices[i];
					double value = sv.values[i];
					if (cols.containsKey(indice)) {
						cols.get(indice).visit(value);
					} else {
						VectorStatCol statCol = new VectorStatCol();
						statCol.visit(value);
						cols.put(indice, statCol);
					}
				}

				if (bCov) {
					if (dotProduction == null) {
						dotProduction = DenseMatrix.zeros(size, size);
					} else {
						if (size > dotProduction.numRows()) {
							DenseMatrix dpNew = DenseMatrix.zeros(size, size);
							if (dotProduction != null) {
								dotProduction = VectorUtil.plusEqual(dpNew, dotProduction);
							}
						}
					}
					for (int i = 0; i < sv.indices.length; i++) {
						double val = sv.values[i];
						int iIdx = sv.indices[i];
						for (int j = 0; j < sv.indices.length; j++) {
							dotProduction.add(iIdx, sv.indices[j], val * sv.values[j]);
						}
					}
				}
			}
			return this;
		} else {
			DenseVector dv = (DenseVector) vec;
			int[] indexs = new int[dv.size()];
			for (int i = 0; i < dv.size(); i++) {
				indexs[i] = i;
			}

			SparseVector sv = new SparseVector(dv.size(), indexs, dv.getData());
			return visit(sv);
		}
	}

	@Override
	public BaseVectorSummarizer visit(Vector vec, double weight) {
		if (vec instanceof SparseVector) {
			SparseVector sv = (SparseVector) vec;
			count++;

			this.colNum = Math.max(this.colNum, sv.n);

			if (sv.nnz != 0) {
				/**
				 * max index + 1 for size.
				 */
				int size = sv.indices[sv.indices.length - 1] + 1;
				for (int i = 0; i < sv.indices.length; i++) {
					int indice = sv.indices[i];
					double value = sv.values[i];
					if (cols.containsKey(indice)) {
						cols.get(indice).visit(value, weight);
					} else {
						VectorStatCol statCol = new VectorStatCol();
						statCol.visit(value, weight);
						cols.put(indice, statCol);
					}
				}

				sumWeight += weight;

				if (bCov) {
					if (dotProduction == null) {
						dotProduction = DenseMatrix.zeros(size, size);
					} else {
						if (size > dotProduction.numRows()) {
							DenseMatrix dpNew = DenseMatrix.zeros(size, size);
							if (dotProduction != null) {
								dotProduction = VectorUtil.plusEqual(dpNew, dotProduction);
							}
						}
					}
					for (int i = 0; i < sv.indices.length; i++) {
						double val = sv.values[i];
						int iIdx = sv.indices[i];
						for (int j = 0; j < sv.indices.length; j++) {
							dotProduction.add(iIdx, sv.indices[j], val * sv.values[j]);
						}
					}
				}
			}
			return this;
		} else {
			DenseVector dv = (DenseVector) vec;
			int[] indexs = new int[dv.size()];
			for (int i = 0; i < dv.size(); i++) {
				indexs[i] = i;
			}

			SparseVector sv = new SparseVector(dv.size(), indexs, dv.getData());
			return visit(sv);
		}
	}

	@Override
	public BaseVectorSummarizer merge(BaseVectorSummarizer srt) {
		if (srt instanceof SparseVectorSummarizer) {
			SparseVectorSummarizer vsrt = (SparseVectorSummarizer) srt;
			count += vsrt.count;
			sumWeight += vsrt.sumWeight;
			colNum = Math.max(vsrt.colNum, colNum);
			for (Map.Entry <Integer, VectorStatCol> entry : vsrt.cols.entrySet()) {
				int indice = entry.getKey();
				if (cols.containsKey(indice)) {
					cols.get(indice).merge(entry.getValue());
				} else {
					cols.put(indice, entry.getValue());
				}
			}
		} else {
			DenseVectorSummarizer vsrt = (DenseVectorSummarizer) srt;
			if (vsrt.count != 0) {
				count += vsrt.count;
				sumWeight += vsrt.sumWeight;
				for (int i = 0; i < vsrt.sum.size(); i++) {
					int indice = i;
					VectorStatCol vsc = new VectorStatCol();
					vsc.numNonZero = 0;
					vsc.sum = vsrt.sum.get(i);
					vsc.sum2 = vsrt.sum2.get(i);
					vsc.min = vsrt.min.get(i);
					vsc.max = vsrt.max.get(i);
					vsc.normL1 = vsrt.normL1.get(i);
					if (cols.containsKey(indice)) {
						cols.get(indice).merge(vsc);
					} else {
						cols.put(indice, vsc);
					}
				}
			}
		}

		if (dotProduction != null && srt.dotProduction != null) {
			int size = srt.dotProduction.numRows();
			if (srt.dotProduction.numRows() > dotProduction.numRows()) {
				DenseMatrix dpNew = DenseMatrix.zeros(size, size);
				dotProduction = VectorUtil.plusEqual(dpNew, dotProduction);
			}
			dotProduction = VectorUtil.plusEqual(dotProduction, srt.dotProduction);
		} else if (dotProduction == null && srt.dotProduction != null) {
			dotProduction = srt.dotProduction.copy();
		}

		return this;
	}

	@Override
	public SparseVectorSummarizer clone() {
		SparseVectorSummarizer vsrt = new SparseVectorSummarizer();
		vsrt.count = count;
		vsrt.sumWeight = sumWeight;
		vsrt.colNum = colNum;
		vsrt.cols = new HashMap();
		for (Map.Entry <Integer, VectorStatCol> entry : cols.entrySet()) {
			vsrt.cols.put(entry.getKey(), entry.getValue());
		}

		if (dotProduction != null) {
			vsrt.dotProduction = dotProduction;
		}
		return vsrt;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder()
			.append("rowNum: " + count);
		for (Map.Entry <Integer, VectorStatCol> entry : cols.entrySet()) {
			sbd.append("\n")
				.append(entry.getKey())
				.append("|")
				.append(entry.getValue().toString());
		}
		return sbd.toString();
	}

	@Override
	public BaseVectorSummary toSummary() {
		SparseVectorSummary summary = new SparseVectorSummary();

		summary.count = count;
		summary.cols = cols;
		summary.colNum = colNum;

		return summary;
	}
}
