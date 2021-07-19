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

import org.apache.flink.ml.common.linalg.DenseMatrix;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.linalg.Vector;
import org.apache.flink.ml.common.linalg.VectorIterator;

import java.util.HashMap;
import java.util.Map;

/**
 * It is summarizer of sparse vector.
 * It uses Map to store median result.
 */
public class SparseVectorSummarizer extends BaseVectorSummarizer {

	/**
	 * colNum.
	 */
	protected int colNum = -1;

	/**
	 * statistics result.
	 */
	protected Map<Integer, VectorStatCol> cols = new HashMap<>();

	/**
	 * default constructor. it will not calculate outerProduct.
	 */
	public SparseVectorSummarizer() {
		this.calculateOuterProduct = false;
	}

	/**
	 * If calculateOuterProduction is true, it will calculate outerProduction.
	 */
	public SparseVectorSummarizer(boolean calculateOuterProduction) {
		this.calculateOuterProduct = calculateOuterProduction;
	}

	/**
	 * update by vector.
	 */
	@Override
	public BaseVectorSummarizer visit(Vector vec) {
		SparseVector sv;

		if (vec instanceof DenseVector) {
			DenseVector dv = (DenseVector) vec;
			int[] indices = new int[dv.size()];
			for (int i = 0; i < dv.size(); i++) {
				indices[i] = i;
			}

			sv = new SparseVector(dv.size(), indices, dv.getData());
		} else {
			sv = (SparseVector) vec;
		}

		count++;

		this.colNum = Math.max(this.colNum, sv.size());

		if (sv.numberOfValues() != 0) {

			//max index + 1 for size.
			VectorIterator iter = sv.iterator();
			while (iter.hasNext()) {
				int index = iter.getIndex();
				double value = iter.getValue();

				if (cols.containsKey(index)) {
					cols.get(index).visit(value);
				} else {
					VectorStatCol statCol = new VectorStatCol();
					statCol.visit(value);
					cols.put(index, statCol);
				}
				iter.next();
			}

			if (calculateOuterProduct) {
				int size = sv.getIndices()[sv.getIndices().length - 1] + 1;

				if (outerProduct == null) {
					outerProduct = DenseMatrix.zeros(size, size);
				} else {
					if (size > outerProduct.numRows()) {
						DenseMatrix dpNew = DenseMatrix.zeros(size, size);
						if (outerProduct != null) {
							outerProduct = VectorSummarizerUtil.plusEqual(dpNew, outerProduct);
						}
					}
				}
				for (int i = 0; i < sv.getIndices().length; i++) {
					double val = sv.getValues()[i];
					int iIdx = sv.getIndices()[i];
					for (int j = 0; j < sv.getIndices().length; j++) {
						outerProduct.add(iIdx, sv.getIndices()[j], val * sv.getValues()[j]);
					}
				}
			}
		}
		return this;
	}

	@Override
	public String toString() {
		StringBuilder sbd = new StringBuilder()
			.append("rowNum: ")
			.append(count);

		for (Map.Entry<Integer, VectorStatCol> entry : cols.entrySet()) {
			sbd.append("\n")
				.append(entry.getKey())
				.append("|")
				.append(entry.getValue().toString());
		}
		return sbd.toString();
	}

	/**
	 * @return summarizer result.
	 */
	@Override
	public BaseVectorSummary toSummary() {
		SparseVectorSummary summary = new SparseVectorSummary();

		summary.count = count;
		summary.cols = cols;
		summary.colNum = colNum;

		return summary;
	}

	public SparseVectorSummarizer copy() {
		SparseVectorSummarizer summarizer = new SparseVectorSummarizer();
		summarizer.count = count;
		summarizer.colNum = colNum;
		for (Map.Entry<Integer, VectorStatCol> entry : cols.entrySet()) {
			summarizer.cols.put(entry.getKey(), entry.getValue());
		}
		return summarizer;
	}

}
