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
import org.apache.flink.ml.common.statistics.StatisticsUtil;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * It is correlation result, which has colNames and correlation values.
 */
public class CorrelationResult {

	public DenseMatrix corr;

	/**
	 * if colNames is null, it is vector correlation result.
	 */
	public String[] colNames;

	@Override
	public String toString() {
		int n = corr.numRows();
		String[] outColNames = new String[1 + n];
		outColNames[0] = "colName";
		if (colNames != null) {
			for (int i = 0; i < n; i++) {
				outColNames[1 + i] = colNames[i];
			}
		} else {
			for (int i = 0; i < n; i++) {
				outColNames[1 + i] = String.valueOf(i);
			}
		}

		List <Row> data = new ArrayList();
		for (int i = 0; i < n; i++) {
			Row row = new Row(outColNames.length);
			row.setField(0, outColNames[1 + i]);
			for (int j = 0; j < n; j++) {
				row.setField(1 + j, corr.get(i, j));
			}
			data.add(row);
		}

		return StatisticsUtil.toString(outColNames, data);
	}
}
