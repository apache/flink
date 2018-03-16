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

import org.apache.flink.api.java.io.TextOutputFormat.TextFormatter;
import org.apache.flink.api.java.tuple.Tuple2;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

/**
 * Writes records that contain an id and a CoordVector. The output format is line-based, i.e. one record is written to a
 * line and terminated with '\n'. Within a line the first '|' character separates the id from the CoordVector. The
 * vector consists of a vector of decimals. The decimals are separated by '|'. The is is the id of a data point or
 * cluster center and the vector the corresponding position (coordinate vector) of the data point or cluster center.
 * Example line: "42|23.23|52.57|74.43| Id: 42 Coordinate vector: (23.23, 52.57, 74.43)
 */
public class PointFormatter implements TextFormatter<Tuple2<Integer, CoordVector>> {
	private static final long serialVersionUID = 1L;

	private final DecimalFormat df = new DecimalFormat("####0.00");
	private final StringBuilder line = new StringBuilder();

	public PointFormatter() {
		DecimalFormatSymbols dfSymbols = new DecimalFormatSymbols();
		dfSymbols.setDecimalSeparator('.');
		this.df.setDecimalFormatSymbols(dfSymbols);
	}

	@Override
	public String format(Tuple2<Integer, CoordVector> value) {
		line.setLength(0);

		line.append(value.f0);

		for (double coord : value.f1.getCoordinates()) {
			line.append('|');
			line.append(df.format(coord));
		}
		line.append('|');

		return line.toString();
	}
}
