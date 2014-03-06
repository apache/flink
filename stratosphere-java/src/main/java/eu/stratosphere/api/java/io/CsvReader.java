/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.java.io;

import java.util.ArrayList;

import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.operators.DataSource;
import eu.stratosphere.api.java.tuple.*;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.core.fs.Path;


/**
 *
 */
public class CsvReader {
	
	private static final int MAX_FIELDS = Tuple.MAX_ARITY;

	private final Path path;
	
	private final ExecutionEnvironment executionContext;
	
	
	private boolean[] includedMask;
	
	private String lineDelimiter = "\n";
	
	private char fieldDelimiter = ',';
	
	
	// --------------------------------------------------------------------------------------------
	
	public CsvReader(Path path, ExecutionEnvironment executionContext) {
		this.path = path;
		this.executionContext = executionContext;
	}
	
	public CsvReader(String path, ExecutionEnvironment executionContext) {
		this.path = new Path(path);
		this.executionContext = executionContext;
	}
	
	public Path getFilePath() {
		return this.path;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public CsvReader lineDelimiter(String delimiter) {
		if (delimiter == null || delimiter.length() == 0) {
			throw new IllegalArgumentException("The delimiter must not be null or an empty string");
		}
		
		this.lineDelimiter = delimiter;
		return this;
	}
	
	public CsvReader fieldDelimiter(char delimiter) {
		this.fieldDelimiter = delimiter;
		return this;
	}
	
	
	public CsvReader includeFields(boolean ... fields) {
		if (fields == null || fields.length == 0) {
			throw new IllegalArgumentException("The set of included fields must not be null or empty.");
		}
		
		this.includedMask = fields;
		return this;
	}

	public CsvReader includeFields(int ... fields) {
		if (fields == null || fields.length == 0) {
			throw new IllegalArgumentException("The set of included fields must not be null or empty.");
		}
		
		return null;
	}
	
	public CsvReader includeFields(String mask) {
		this.includedMask = new boolean[mask.length()];
		
		for (int i = 0; i < mask.length(); i++) {
			char c = mask.charAt(i);
			if (c == '1' || c == 'T') {
				this.includedMask[i] = true;
			} else if (c != 0 && c != 'F') {
				throw new IllegalArgumentException("Mask string may contain only '0' and '1'.");
			}
		}
		
		return this;
	}
	
	public CsvReader includeFields(long mask) {
		ArrayList<Boolean> fields = new ArrayList<Boolean>();
		
		int highestNum = 0;
		for (int i = 0; i < 64; i++) {
			long bitMask = 0x1l << i;
			if ((mask & bitMask) != 0) {
				fields.add(true);
				highestNum = i;
			} else {
				fields.add(false);
			}
		}
		
		this.includedMask = new boolean[highestNum + 1];
		for (int i = 0; i < highestNum; i++) {
			this.includedMask[i] = fields.get(i);
		}
		
		return this;
	}
	
	// --------------------------------------------------------------------------------------------
	
	public <T1> DataSource<Tuple1<T1>> types(Class<T1> type1) {
		TupleTypeInfo<Tuple1<T1>> types = TupleTypeInfo.getBasicTupleTypeInfo(type1);
		return new DataSource<Tuple1<T1>>(executionContext, new CsvInputFormat<Tuple1<T1>>(path, lineDelimiter, fieldDelimiter, type1), types);
	}
	
	public <T1, T2> DataSource<Tuple2<T1, T2>> types(Class<T1> type1, Class<T2> type2) {
		TupleTypeInfo<Tuple2<T1, T2>> types = TupleTypeInfo.getBasicTupleTypeInfo(type1, type2);
		return new DataSource<Tuple2<T1, T2>>(executionContext, new CsvInputFormat<Tuple2<T1, T2>>(path, lineDelimiter, fieldDelimiter, type1, type2), types);
	}
	
	public <T1, T2, T3> DataSource<Tuple3<T1, T2, T3>> types(Class<T1> type1, Class<T2> type2, Class<T3> type3) {
		TupleTypeInfo<Tuple3<T1, T2, T3>> types = TupleTypeInfo.getBasicTupleTypeInfo(type1, type2, type3);
		return new DataSource<Tuple3<T1, T2, T3>>(executionContext, new CsvInputFormat<Tuple3<T1, T2, T3>>(path, lineDelimiter, fieldDelimiter, type1, type2, type3), types);
	}
	
	public <T1, T2, T3, T4> DataSource<Tuple4<T1, T2, T3, T4>> types(Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4) {
		TupleTypeInfo<Tuple4<T1, T2, T3, T4>> types = TupleTypeInfo.getBasicTupleTypeInfo(type1, type2, type3, type4);
		return new DataSource<Tuple4<T1, T2, T3, T4>>(executionContext, new CsvInputFormat<Tuple4<T1, T2, T3, T4>>(path, lineDelimiter, fieldDelimiter, type1, type2, type3, type4), types);
	}
	
	public <T1, T2, T3, T4, T5> DataSource<Tuple5<T1, T2, T3, T4, T5>> types(Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5) {
		TupleTypeInfo<Tuple5<T1, T2, T3, T4, T5>> types = TupleTypeInfo.getBasicTupleTypeInfo(type1, type2, type3, type4, type5);
		return new DataSource<Tuple5<T1, T2, T3, T4, T5>>(executionContext, new CsvInputFormat<Tuple5<T1, T2, T3, T4, T5>>(path, lineDelimiter, fieldDelimiter, type1, type2, type3, type4, type5), types);
	}
	
	public <T1, T2, T3, T4, T5, T6> DataSource<Tuple6<T1, T2, T3, T4, T5, T6>> types(Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T5> type6) {
		TupleTypeInfo<Tuple6<T1, T2, T3, T4, T5, T6>> types = TupleTypeInfo.getBasicTupleTypeInfo(type1, type2, type3, type4, type5, type6);
		return new DataSource<Tuple6<T1, T2, T3, T4, T5, T6>>(executionContext, new CsvInputFormat<Tuple6<T1, T2, T3, T4, T5, T6>>(path, lineDelimiter, fieldDelimiter, type1, type2, type3, type4, type5, type6), types);
	}
}
