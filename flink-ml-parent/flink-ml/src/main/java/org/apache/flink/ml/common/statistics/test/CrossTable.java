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

package org.apache.flink.ml.common.statistics.test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Cross Table.
 */
public class CrossTable {

	public List <String> colTags = new ArrayList <>();
	public List <String> rowTags = new ArrayList <>();
	public List <List <Long>> data = new ArrayList <>();

	//convert map to crosstable
	public static CrossTable convert(Map <String[], Long> maps) {
		CrossTable cross = new CrossTable();

		Set <String[]> sets = maps.keySet();
		for (String[] set : sets) {
			if (!cross.rowTags.contains(set[0])) {
				cross.rowTags.add(set[0]);
			}
			if (!cross.colTags.contains(set[1])) {
				cross.colTags.add(set[1]);
			}
		}

		int rowLen = cross.rowTags.size();
		int colLen = cross.colTags.size();

		for (int i = 0; i < rowLen; i++) {
			List <Long> rowData = new ArrayList <>();
			for (int j = 0; j < colLen; j++) {
				rowData.add(0L);
			}
			cross.data.add(rowData);
		}
		for (String[] set : sets) {
			int rowIdx = cross.rowTags.indexOf(set[0]);
			int colIdx = cross.colTags.indexOf(set[1]);
			cross.data.get(rowIdx).set(colIdx, maps.get(set));
		}
		return cross;
	}

	//combin left and right
	public static CrossTable combin(CrossTable left, CrossTable right) {
		CrossTable cross = new CrossTable();
		cross.rowTags = new ArrayList <>();
		cross.rowTags.addAll(left.rowTags);
		cross.colTags = new ArrayList <>();
		cross.colTags.addAll(left.colTags);

		for (String row : right.rowTags) {
			if (!cross.rowTags.contains(row)) {
				cross.rowTags.add(row);
			}
		}
		for (String col : right.colTags) {
			if (!cross.colTags.contains(col)) {
				cross.colTags.add(col);
			}
		}

		for (String row : cross.rowTags) {
			List <Long> rowData = new ArrayList <>();
			for (String col : cross.colTags) {
				long tmp = 0;
				if (left.rowTags.contains(row) && left.colTags.contains(col)) {
					tmp += left.data.get(left.rowTags.indexOf(row)).get(left.colTags.indexOf(col));
				}
				if (right.rowTags.contains(row) && right.colTags.contains(col)) {
					tmp += right.data.get(right.rowTags.indexOf(row)).get(right.colTags.indexOf(col));
				}
				rowData.add(tmp);
			}
			cross.data.add(rowData);
		}

		return cross;
	}
}
