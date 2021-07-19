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

package org.apache.flink.ml.operator.common.statistics.basicstatistic;

import org.apache.flink.ml.common.linalg.DenseMatrix;
import org.apache.flink.ml.common.utils.TableUtil;
import org.apache.flink.types.Row;

import java.util.ArrayList;

/**
 * It is correlation result, which has colNames and correlation values.
 */
public class CorrelationResult {

	/**
	 * correlation data.
	 */
	public DenseMatrix correlation;

	/**
	 * If it is vector correlation, colNames is null.
	 */
	public String[] colNames;

	@Override
	public String toString() {
		int n = correlation.numRows();
		String[] outColNames = new String[1 + n];
		outColNames[0] = "colName";
		if (colNames != null) {
			System.arraycopy(colNames, 0, outColNames, 1, n);
		} else {
			for (int i = 0; i < n; i++) {
				outColNames[1 + i] = String.valueOf(i);
			}
		}

		ArrayList<Row> data = new ArrayList<>();
		for (int i = 0; i < n; i++) {
			Row row = new Row(outColNames.length);
			row.setField(0, outColNames[1 + i]);
			for (int j = 0; j < n; j++) {
				row.setField(1 + j, correlation.get(i, j));
			}
			data.add(row);
		}

		return TableUtil.format(outColNames, data);
	}
}
