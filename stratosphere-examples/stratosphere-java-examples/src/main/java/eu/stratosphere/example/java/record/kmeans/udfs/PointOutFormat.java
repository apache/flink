/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.example.java.record.kmeans.udfs;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import eu.stratosphere.api.java.record.io.DelimitedOutputFormat;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;

/**
 * Writes records that contain an id and a CoordVector.
 * The output format is line-based, i.e. one record is written to
 * a line and terminated with '\n'. Within a line the first '|' character
 * separates the id from the CoordVector. The vector consists of a vector of
 * decimals. The decimals are separated by '|'. The is is the id of a data
 * point or cluster center and the vector the corresponding position
 * (coordinate vector) of the data point or cluster center. Example line:
 * "42|23.23|52.57|74.43| Id: 42 Coordinate vector: (23.23, 52.57, 74.43)
 */
public class PointOutFormat extends DelimitedOutputFormat {
	private static final long serialVersionUID = 1L;
	
	private final DecimalFormat df = new DecimalFormat("####0.00");
	private final StringBuilder line = new StringBuilder();
	
	
	public PointOutFormat() {
		DecimalFormatSymbols dfSymbols = new DecimalFormatSymbols();
		dfSymbols.setDecimalSeparator('.');
		this.df.setDecimalFormatSymbols(dfSymbols);
	}
	
	@Override
	public int serializeRecord(Record record, byte[] target) {
		
		line.setLength(0);
		
		IntValue centerId = record.getField(0, IntValue.class);
		CoordVector centerPos = record.getField(1, CoordVector.class);
		
		
		line.append(centerId.getValue());

		for (double coord : centerPos.getCoordinates()) {
			line.append('|');
			line.append(df.format(coord));
		}
		line.append('|');
		
		byte[] byteString = line.toString().getBytes();
		
		if (byteString.length <= target.length) {
			System.arraycopy(byteString, 0, target, 0, byteString.length);
			return byteString.length;
		}
		else {
			return -byteString.length;
		}
	}
}