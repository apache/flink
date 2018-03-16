/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.util;

import org.apache.flink.api.common.io.DelimitedInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Generates records with an id and a and CoordVector. The input format is line-based, i.e. one record is read from one
 * line which is terminated by '\n'. Within a line the first '|' character separates the id from the CoordVector. The
 * vector consists of a vector of decimals. The decimals are separated by '|' as well. The id is the id of a data point
 * or cluster center and the CoordVector the corresponding position (coordinate vector) of the data point or cluster
 * center. Example line: "42|23.23|52.57|74.43| Id: 42 Coordinate vector: (23.23, 52.57, 74.43)
 */
public class PointInFormat extends DelimitedInputFormat<Tuple2<Integer, CoordVector>> {
	private static final long serialVersionUID = 1L;

	private final List<Double> dimensionValues = new ArrayList<Double>();
	private double[] pointValues = new double[0];

	@Override
	public Tuple2<Integer, CoordVector> readRecord(Tuple2<Integer, CoordVector> reuse, byte[] bytes, int offset, int numBytes) throws IOException {

		final int limit = offset + numBytes;

		int id = -1;
		int value = 0;
		int fractionValue = 0;
		int fractionChars = 0;
		boolean negative = false;

		this.dimensionValues.clear();

		for (int pos = offset; pos < limit; pos++) {
			if (bytes[pos] == '|') {
				// check if id was already set
				if (id == -1) {
					id = value;
				} else {
					double v = value + ((double) fractionValue) * Math.pow(10, (-1 * (fractionChars - 1)));
					this.dimensionValues.add(negative ? -v : v);
				}
				// reset value
				value = 0;
				fractionValue = 0;
				fractionChars = 0;
				negative = false;
			} else if (bytes[pos] == '.') {
				fractionChars = 1;
			} else if (bytes[pos] == '-') {
				negative = true;
			} else {
				if (fractionChars == 0) {
					value *= 10;
					value += bytes[pos] - '0';
				} else {
					fractionValue *= 10;
					fractionValue += bytes[pos] - '0';
					fractionChars++;
				}
			}
		}

		// set the ID
		reuse.f0 = id;

		// set the data points
		if (this.pointValues.length != this.dimensionValues.size()) {
			this.pointValues = new double[this.dimensionValues.size()];
		}
		for (int i = 0; i < this.pointValues.length; i++) {
			this.pointValues[i] = this.dimensionValues.get(i);
		}

		reuse.f1 = new CoordVector(this.pointValues);
		return reuse;
	}
}
