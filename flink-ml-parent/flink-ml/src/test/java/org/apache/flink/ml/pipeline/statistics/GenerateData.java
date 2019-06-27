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

package org.apache.flink.ml.pipeline.statistics;

import org.apache.flink.ml.common.MLSession;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * GenerateData class for test.
 */
public class GenerateData {

	public static Table getBatchTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {1.0, 2.0}),
				Row.of(new Object[] {-1.0, -3.0}),
				Row.of(new Object[] {4.0, 2.0}),
				Row.of(new Object[] {null, null}),
			};

		String[] colNames = new String[] {"f0", "f1"};

		return MLSession.createBatchTable(Arrays.asList(testArray), colNames);
	}

	public static Table getStreamTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {1.0, 2.0}),
				Row.of(new Object[] {-1.0, -3.0}),
				Row.of(new Object[] {4.0, 2.0}),
				Row.of(new Object[] {null, null}),
			};

		String[] colNames = new String[] {"f0", "f1"};

		return MLSession.createStreamTable(Arrays.asList(testArray), colNames);
	}

	public static Table getMultiTypeBatchTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", 1L, 1, 2.0, true}),
				Row.of(new Object[] {null, 2L, 2, -3.0, true}),
				Row.of(new Object[] {"c", null, null, 2.0, false}),
				Row.of(new Object[] {"a", 0L, 0, null, null}),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

		return MLSession.createBatchTable(Arrays.asList(testArray), colNames);
	}

	public static Table getMultiTypeStreamTable() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", 1L, 1, 2.0, true}),
				Row.of(new Object[] {null, 2L, 2, -3.0, true}),
				Row.of(new Object[] {"c", null, null, 2.0, false}),
				Row.of(new Object[] {"a", 0L, 0, null, null}),
			};

		String[] colNames = new String[] {"f_string", "f_long", "f_int", "f_double", "f_boolean"};

		return MLSession.createStreamTable(Arrays.asList(testArray), colNames);
	}

	public static Table getSparseBatch() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"0:1.0,1:2.0"}),
				Row.of(new Object[] {"0:-1.0,1:-3.0"}),
				Row.of(new Object[] {"0:4.0,1:2.0"}),
				Row.of(new Object[] {""}),
			};

		String selectedColName = "vec";
		String[] colNames = new String[] {selectedColName};

		return MLSession.createBatchTable(Arrays.asList(testArray), colNames);
	}

	public static Table getSparseStream() {
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

		return MLSession.createStreamTable(Arrays.asList(testArray), colNames);
	}

	public static Table getDenseBatch() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1.0,2.0"}),
				Row.of(new Object[] {"-1.0,-3.0"}),
				Row.of(new Object[] {"4.0,2.0"}),
			};

		String selectedColName = "vec";
		String[] colNames = new String[] {selectedColName};

		return MLSession.createBatchTable(Arrays.asList(testArray), colNames);
	}

	public static Table getDenseStream() {
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

		return MLSession.createStreamTable(Arrays.asList(testArray), colNames);
	}

	public static Table getDenseBatchWithStreamLabel() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"1.0,2.0,4.0", "a"}),
				Row.of(new Object[] {"-1.0,-3.0,4.0", "a"}),
				Row.of(new Object[] {"4.0,2.0,3.0", "b"}),
				Row.of(new Object[] {"3.4,5.1,5.0", "b"})
			};

		String[] colNames = new String[] {"vec", "label"};

		return MLSession.createBatchTable(Arrays.asList(testArray), colNames);
	}

	public static Table getDenseBatchWithDoubleLabel() {

		Row[] testArray = new Row[] {
			Row.of(new Object[] {7, "0.0, 0.0, 18.0, 1.0", 1.0}),
			Row.of(new Object[] {8, "0.0, 1.0, 12.0, 0.0", 0.0}),
			Row.of(new Object[] {9, "1.0, 0.0, 15.0, 0.1", 0.0})
		};

		String[] colNames = new String[] {"id", "features", "clicked"};

		return MLSession.createBatchTable(Arrays.asList(testArray), colNames);
	}

	public static Table genTableWithStringLabel() {
		Row[] testArray =
			new Row[] {
				Row.of(new Object[] {"a", 1, 1.1, 1.2}),
				Row.of(new Object[] {"b", -2, 0.9, 1.0}),
				Row.of(new Object[] {"c", 100, -0.01, 1.0}),
				Row.of(new Object[] {"d", -99, 100.9, 0.1}),
				Row.of(new Object[] {"a", 1, 1.1, 1.2}),
				Row.of(new Object[] {"b", -2, 0.9, 1.0}),
				Row.of(new Object[] {"c", 100, -0.01, 0.2}),
				Row.of(new Object[] {"d", -99, 100.9, 0.3})
			};

		String[] colNames = new String[] {"col1", "col2", "col3", "col4"};

		return MLSession.createBatchTable(Arrays.asList(testArray), colNames);
	}

}
