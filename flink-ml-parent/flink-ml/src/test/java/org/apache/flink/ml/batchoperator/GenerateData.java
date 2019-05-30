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

package org.apache.flink.ml.batchoperator;

import org.apache.flink.ml.batchoperator.source.MemSourceBatchOp;
import org.apache.flink.ml.streamoperator.StreamOperator;
import org.apache.flink.ml.streamoperator.source.MemSourceStreamOp;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * Genereate data for test cases.
 */
public class GenerateData {

	public static String getSelectedColName() {
		return "vec";
	}

	public static String[] getSelectedColNames() {
		return new String[] {"f0", "f1"};
	}

	public static BatchOperator getTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {1.0, 2.0}),
				Row.of(new Object[] {-1.0, -3.0}),
				Row.of(new Object[] {4.0, 2.0}),
				Row.of(new Object[] {null, null}),
			};

		String[] colNames = new String[] {"f0", "f1"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		return inOp;
	}

	public static BatchOperator getMultiTypeTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", 1L, 1, 2.0, true}),
				Row.of(new Object[] {null, 2L, 2, -3.0, true}),
				Row.of(new Object[] {"c", null, null, 2.0, false}),
				Row.of(new Object[] {"a", 0L, 0, null, null}),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		return inOp;
	}

	public static StreamOperator getMultiTypeSTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", 1L, 1, 2.0, true}),
				Row.of(new Object[] {null, 2L, 2, -3.0, true}),
				Row.of(new Object[] {"c", null, null, 2.0, false}),
				Row.of(new Object[] {"a", 0L, 0, null, null}),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colNames);

		return inOp;
	}

	public static BatchOperator getDenseVector() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1.0,2.0"}),
				Row.of(new Object[] {"-1.0,-3.0"}),
				Row.of(new Object[] {"4.0,2.0"}),
				//                        Row.of(new Object[]{""}),
				//                        Row.of(new Object[]{null})
			};

		String selectedColName = "vec";
		String[] colNames = new String[] {selectedColName};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		return inOp;
	}

	public static BatchOperator getDenseVector2() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1.0,2.0,4.0", "a"}),
				Row.of(new Object[] {"-1.0,-3.0,4.0", "a"}),
				Row.of(new Object[] {"4.0,2.0,3.0", "b"}),
				Row.of(new Object[] {"3.4,5.1,5.0", "b"})
			};

		String[] colNames = new String[] {"vec", "label"};

		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		return inOp;
	}

	public static BatchOperator getSparseVector() throws Exception {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"0:1.0,1:2.0"}),
				Row.of(new Object[] {"0:-1.0,1:-3.0"}),
				Row.of(new Object[] {"0:4.0,1:2.0"}),
				Row.of(new Object[] {""}),
				//                        Row.of(new Object[]{null})
			};

		String selectedColName = "vec";
		String[] colNames = new String[] {selectedColName};
		MemSourceBatchOp inOp = new MemSourceBatchOp(Arrays.asList(testArray), colNames);

		return inOp;
	}

	public static StreamOperator getSTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {1.0, 2.0}),
				Row.of(new Object[] {-1.0, -3.0}),
				Row.of(new Object[] {4.0, 2.0}),
				Row.of(new Object[] {null, null}),
			};

		String[] colNames = new String[] {"f0", "f1"};
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colNames);

		return inOp;
	}

	public static StreamOperator getSDenseVector() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1.0,2.0"}),
				Row.of(new Object[] {"-1.0,-3.0"}),
				Row.of(new Object[] {"4.0,2.0"}),
				Row.of(new Object[] {""}),
				Row.of(new Object[] {null})
			};

		String selectedColName = "vec";
		String[] colNames = new String[] {selectedColName};
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colNames);

		return inOp;
	}

	public static StreamOperator getSSparseVector() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"0:1.0,1:2.0"}),
				Row.of(new Object[] {"0:-1.0,1:-3.0"}),
				Row.of(new Object[] {"0:4.0,1:2.0"}),
				Row.of(new Object[] {""}),
				Row.of(new Object[] {null})
			};

		String selectedColName = "vec";
		String[] colNames = new String[] {selectedColName};
		MemSourceStreamOp inOp = new MemSourceStreamOp(Arrays.asList(testArray), colNames);

		return inOp;
	}
}
