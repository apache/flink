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

package eu.stratosphere.example.java.record.triangles.io;

import eu.stratosphere.api.java.record.io.DelimitedInputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;


/**
 * Input format that reads edges augmented with vertex degrees. The data to be read is assumed to be in
 * the format <code>v1,d1|v2,d2\n</code>, where <code>v1</code> and <code>v2</code> are the IDs of the first and
 * second vertex, while <code>d1</code> and <code>d2</code> are the vertex degrees.
 * <p>
 * The result record holds the fields in the sequence <code>(v1, v2, d1, d2)</code>.
 * <p>
 * The delimiters are configurable. The default delimiter between vertex ID and
 * vertex degree is the comma (<code>,</code>). The default delimiter between the two vertices is
 * the vertical bar (<code>|</code>).
 */
public final class EdgeWithDegreesInputFormat extends DelimitedInputFormat {
	private static final long serialVersionUID = 1L;
	
	public static final String VERTEX_DELIMITER_CHAR = "edgeinput.vertexdelimiter";
	public static final String DEGREE_DELIMITER_CHAR = "edgeinput.degreedelimiter";
	
	private final IntValue v1 = new IntValue();
	private final IntValue v2 = new IntValue();
	private final IntValue d1 = new IntValue();
	private final IntValue d2 = new IntValue();
	
	private char vertexDelimiter;
	private char degreeDelimiter;
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public Record readRecord(Record target, byte[] bytes, int offset, int numBytes) {
		final int limit = offset + numBytes;
		int firstV = 0, secondV = 0;
		int firstD = 0, secondD = 0;
		
		final char vertexDelimiter = this.vertexDelimiter;
		final char degreeDelimiter = this.degreeDelimiter;
		
		int pos = offset;
		
		// read the first vertex ID
		while (pos < limit && bytes[pos] != degreeDelimiter) {
			firstV = firstV * 10 + (bytes[pos++] - '0');
		}
		
		pos += 1;// skip the delimiter
		
		// read the first vertex degree
		while (pos < limit && bytes[pos] != vertexDelimiter) {
			firstD = firstD * 10 + (bytes[pos++] - '0');
		}
		
		pos += 1;// skip the delimiter
		
		// read the second vertex ID
		while (pos < limit && bytes[pos] != degreeDelimiter) {
			secondV = secondV * 10 + (bytes[pos++] - '0');
		}
		
		pos += 1;// skip the delimiter
		
		// read the second vertex degree
		while (pos < limit) {
			secondD = secondD * 10 + (bytes[pos++] - '0');
		}
		
		if (firstV <= 0 || secondV <= 0 || firstV == secondV) {
			return null;
		}
		
		v1.setValue(firstV);
		v2.setValue(secondV);
		d1.setValue(firstD);
		d2.setValue(secondD);
		
		target.setField(0, v1);
		target.setField(1, v2);
		target.setField(2, d1);
		target.setField(3, d2);
		
		return target;
	}
	
	// --------------------------------------------------------------------------------------------
	

	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		this.vertexDelimiter = (char) parameters.getInteger(VERTEX_DELIMITER_CHAR, '|');
		this.degreeDelimiter = (char) parameters.getInteger(DEGREE_DELIMITER_CHAR, ',');
	}
}
