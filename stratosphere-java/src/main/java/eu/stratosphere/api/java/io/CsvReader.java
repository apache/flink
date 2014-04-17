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

import org.apache.commons.lang3.Validate;

import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.operators.DataSource;
import eu.stratosphere.api.java.tuple.*;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.core.fs.Path;


/**
 *
 */
public class CsvReader {

	private final Path path;
	
	private final ExecutionEnvironment executionContext;
	
	
	private boolean[] includedMask;
	
	private String lineDelimiter = CsvInputFormat.DEFAULT_LINE_DELIMITER;
	
	private char fieldDelimiter = CsvInputFormat.DEFAULT_FIELD_DELIMITER;

	private boolean skipFirstLineAsHeader = false;
	
	
	// --------------------------------------------------------------------------------------------
	
	public CsvReader(Path filePath, ExecutionEnvironment executionContext) {
		Validate.notNull(filePath, "The file path may not be null.");
		Validate.notNull(executionContext, "The execution context may not be null.");
		
		this.path = filePath;
		this.executionContext = executionContext;
	}
	
	public CsvReader(String filePath, ExecutionEnvironment executionContext) {
		this(new Path(Validate.notNull(filePath, "The file path may not be null.")), executionContext);
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
			} else if (c != '0' && c != 'F') {
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

	public CsvReader ignoreFirstLine() {
		skipFirstLineAsHeader = true;
		return this;
	}
	
	// --------------------------------------------------------------------------------------------
	// Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	private void configureInputFormat(CsvInputFormat<?> format, Class<?>... types) {
		format.setDelimiter(this.lineDelimiter);
		format.setFieldDelimiter(this.fieldDelimiter);
		format.setSkipFirstLineAsHeader(skipFirstLineAsHeader);
		if (this.includedMask == null) {
			format.setFieldTypes(types);
		} else {
			format.setFields(this.includedMask, types);
		}
	}
	
	// --------------------------------------------------------------------------------------------	
	// The following lines are generated.
	// --------------------------------------------------------------------------------------------	
	// BEGIN_OF_TUPLE_DEPENDENT_CODE
	// GENERATED FROM eu.stratosphere.api.java.tuple.TupleGenerator.

	public <T0> DataSource<Tuple1<T0>> types(Class<T0> type0) {
		TupleTypeInfo<Tuple1<T0>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0);
		CsvInputFormat<Tuple1<T0>> inputFormat = new CsvInputFormat<Tuple1<T0>>(path);
		configureInputFormat(inputFormat, type0);
		return new DataSource<Tuple1<T0>>(executionContext, inputFormat, types);
	}

	public <T0, T1> DataSource<Tuple2<T0, T1>> types(Class<T0> type0, Class<T1> type1) {
		TupleTypeInfo<Tuple2<T0, T1>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1);
		CsvInputFormat<Tuple2<T0, T1>> inputFormat = new CsvInputFormat<Tuple2<T0, T1>>(path);
		configureInputFormat(inputFormat, type0, type1);
		return new DataSource<Tuple2<T0, T1>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2> DataSource<Tuple3<T0, T1, T2>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2) {
		TupleTypeInfo<Tuple3<T0, T1, T2>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2);
		CsvInputFormat<Tuple3<T0, T1, T2>> inputFormat = new CsvInputFormat<Tuple3<T0, T1, T2>>(path);
		configureInputFormat(inputFormat, type0, type1, type2);
		return new DataSource<Tuple3<T0, T1, T2>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3> DataSource<Tuple4<T0, T1, T2, T3>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3) {
		TupleTypeInfo<Tuple4<T0, T1, T2, T3>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3);
		CsvInputFormat<Tuple4<T0, T1, T2, T3>> inputFormat = new CsvInputFormat<Tuple4<T0, T1, T2, T3>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3);
		return new DataSource<Tuple4<T0, T1, T2, T3>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4> DataSource<Tuple5<T0, T1, T2, T3, T4>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4) {
		TupleTypeInfo<Tuple5<T0, T1, T2, T3, T4>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4);
		CsvInputFormat<Tuple5<T0, T1, T2, T3, T4>> inputFormat = new CsvInputFormat<Tuple5<T0, T1, T2, T3, T4>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4);
		return new DataSource<Tuple5<T0, T1, T2, T3, T4>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5> DataSource<Tuple6<T0, T1, T2, T3, T4, T5>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5) {
		TupleTypeInfo<Tuple6<T0, T1, T2, T3, T4, T5>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5);
		CsvInputFormat<Tuple6<T0, T1, T2, T3, T4, T5>> inputFormat = new CsvInputFormat<Tuple6<T0, T1, T2, T3, T4, T5>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5);
		return new DataSource<Tuple6<T0, T1, T2, T3, T4, T5>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6> DataSource<Tuple7<T0, T1, T2, T3, T4, T5, T6>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6) {
		TupleTypeInfo<Tuple7<T0, T1, T2, T3, T4, T5, T6>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6);
		CsvInputFormat<Tuple7<T0, T1, T2, T3, T4, T5, T6>> inputFormat = new CsvInputFormat<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6);
		return new DataSource<Tuple7<T0, T1, T2, T3, T4, T5, T6>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7> DataSource<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7) {
		TupleTypeInfo<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7);
		CsvInputFormat<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>> inputFormat = new CsvInputFormat<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7);
		return new DataSource<Tuple8<T0, T1, T2, T3, T4, T5, T6, T7>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8> DataSource<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8) {
		TupleTypeInfo<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8);
		CsvInputFormat<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>> inputFormat = new CsvInputFormat<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8);
		return new DataSource<Tuple9<T0, T1, T2, T3, T4, T5, T6, T7, T8>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9> DataSource<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9) {
		TupleTypeInfo<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9);
		CsvInputFormat<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>> inputFormat = new CsvInputFormat<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9);
		return new DataSource<Tuple10<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10> DataSource<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10) {
		TupleTypeInfo<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10);
		CsvInputFormat<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>> inputFormat = new CsvInputFormat<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10);
		return new DataSource<Tuple11<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11> DataSource<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11) {
		TupleTypeInfo<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11);
		CsvInputFormat<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>> inputFormat = new CsvInputFormat<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11);
		return new DataSource<Tuple12<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12> DataSource<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12) {
		TupleTypeInfo<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12);
		CsvInputFormat<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>> inputFormat = new CsvInputFormat<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12);
		return new DataSource<Tuple13<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13> DataSource<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13) {
		TupleTypeInfo<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13);
		CsvInputFormat<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>> inputFormat = new CsvInputFormat<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13);
		return new DataSource<Tuple14<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14> DataSource<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14) {
		TupleTypeInfo<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14);
		CsvInputFormat<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>> inputFormat = new CsvInputFormat<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14);
		return new DataSource<Tuple15<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15> DataSource<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15) {
		TupleTypeInfo<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15);
		CsvInputFormat<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>> inputFormat = new CsvInputFormat<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15);
		return new DataSource<Tuple16<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16> DataSource<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16) {
		TupleTypeInfo<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16);
		CsvInputFormat<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>> inputFormat = new CsvInputFormat<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16);
		return new DataSource<Tuple17<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17> DataSource<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17) {
		TupleTypeInfo<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17);
		CsvInputFormat<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>> inputFormat = new CsvInputFormat<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17);
		return new DataSource<Tuple18<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18> DataSource<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18) {
		TupleTypeInfo<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18);
		CsvInputFormat<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>> inputFormat = new CsvInputFormat<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18);
		return new DataSource<Tuple19<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19> DataSource<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19) {
		TupleTypeInfo<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19);
		CsvInputFormat<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>> inputFormat = new CsvInputFormat<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19);
		return new DataSource<Tuple20<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20> DataSource<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20) {
		TupleTypeInfo<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20);
		CsvInputFormat<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>> inputFormat = new CsvInputFormat<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20);
		return new DataSource<Tuple21<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20>>(executionContext, inputFormat, types);
	}

	public <T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21> DataSource<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2, Class<T3> type3, Class<T4> type4, Class<T5> type5, Class<T6> type6, Class<T7> type7, Class<T8> type8, Class<T9> type9, Class<T10> type10, Class<T11> type11, Class<T12> type12, Class<T13> type13, Class<T14> type14, Class<T15> type15, Class<T16> type16, Class<T17> type17, Class<T18> type18, Class<T19> type19, Class<T20> type20, Class<T21> type21) {
		TupleTypeInfo<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21);
		CsvInputFormat<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>> inputFormat = new CsvInputFormat<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(path);
		configureInputFormat(inputFormat, type0, type1, type2, type3, type4, type5, type6, type7, type8, type9, type10, type11, type12, type13, type14, type15, type16, type17, type18, type19, type20, type21);
		return new DataSource<Tuple22<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19, T20, T21>>(executionContext, inputFormat, types);
	}

	// END_OF_TUPLE_DEPENDENT_CODE
}
