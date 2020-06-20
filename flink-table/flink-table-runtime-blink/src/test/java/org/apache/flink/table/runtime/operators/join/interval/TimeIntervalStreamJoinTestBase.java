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

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.util.RowDataHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Base Test for all subclass of {@link TimeIntervalJoin}.
 */
abstract class TimeIntervalStreamJoinTestBase {
	RowDataTypeInfo rowType = new RowDataTypeInfo(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));

	private RowDataTypeInfo outputRowType = new RowDataTypeInfo(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));
	RowDataHarnessAssertor assertor = new RowDataHarnessAssertor(outputRowType.getFieldTypes());

	private String funcCode =
			"public class IntervalJoinFunction\n" +
					"    extends org.apache.flink.api.common.functions.RichFlatJoinFunction {\n" +
					"  final org.apache.flink.table.data.JoinedRowData joinedRow = new org.apache.flink.table.data.JoinedRowData();\n" +

					"  public IntervalJoinFunction(Object[] references) throws Exception {}\n" +

					"  @Override\n" +
					"  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {}\n" +

					"  @Override\n" +
					"  public void join(Object _in1, Object _in2, org.apache.flink.util.Collector c) throws Exception {\n" +
					"    org.apache.flink.table.data.RowData in1 = (org.apache.flink.table.data.RowData) _in1;\n" +
					"    org.apache.flink.table.data.RowData in2 = (org.apache.flink.table.data.RowData) _in2;\n" +
					"    joinedRow.replace(in1,in2);\n" +
					"    c.collect(joinedRow);\n" +
					"  }\n" +

					"  @Override\n" +
					"  public void close() throws Exception {}\n" +
					"}\n";

	GeneratedFunction<FlatJoinFunction<RowData, RowData, RowData>> generatedFunction = new GeneratedFunction(
			"IntervalJoinFunction", funcCode, new Object[0]);

}
