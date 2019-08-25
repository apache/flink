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

package org.apache.flink.table.runtime.operators.join;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.generated.GeneratedFunction;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.util.BaseRowHarnessAssertor;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;

/**
 * Base Test for all subclass of {@link TimeBoundedStreamJoin}.
 */
abstract class TimeBoundedStreamJoinTestBase {
	BaseRowTypeInfo rowType = new BaseRowTypeInfo(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));

	private BaseRowTypeInfo outputRowType = new BaseRowTypeInfo(new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH),
			new BigIntType(), new VarCharType(VarCharType.MAX_LENGTH));
	BaseRowHarnessAssertor assertor = new BaseRowHarnessAssertor(outputRowType.getFieldTypes());

	private String funcCode =
			"public class WindowJoinFunction\n" +
					"    extends org.apache.flink.api.common.functions.RichFlatJoinFunction {\n" +
					"  final org.apache.flink.table.dataformat.JoinedRow joinedRow = new org.apache.flink.table.dataformat.JoinedRow();\n" +

					"  public WindowJoinFunction(Object[] references) throws Exception {}\n" +

					"  @Override\n" +
					"  public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {}\n" +

					"  @Override\n" +
					"  public void join(Object _in1, Object _in2, org.apache.flink.util.Collector c) throws Exception {\n" +
					"    org.apache.flink.table.dataformat.BaseRow in1 = (org.apache.flink.table.dataformat.BaseRow) _in1;\n" +
					"    org.apache.flink.table.dataformat.BaseRow in2 = (org.apache.flink.table.dataformat.BaseRow) _in2;\n" +
					"    joinedRow.replace(in1,in2);\n" +
					"    c.collect(joinedRow);\n" +
					"  }\n" +

					"  @Override\n" +
					"  public void close() throws Exception {}\n" +
					"}\n";

	GeneratedFunction<FlatJoinFunction<BaseRow, BaseRow, BaseRow>> generatedFunction = new GeneratedFunction(
			"WindowJoinFunction", funcCode, new Object[0]);

}
