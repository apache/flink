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

package org.apache.flink.table.runtime.stream.table;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/**
 * End to end tests for {@link org.apache.flink.table.api.TableEnvironment#fromValues}.
 */
public class ValuesITCase extends AbstractTestBase {
	@Test
	public void testTypeConversions() throws Exception {
		List<Row> data = Arrays.asList(
			Row.of(1, "ABC", java.sql.Timestamp.valueOf("2000-12-12 12:30:57.12"), Row.of(1, "ABC", Arrays.asList(1, 2, 3))),
			Row.of(Math.PI, "ABC", LocalDateTime.parse("2000-12-12T12:30:57.123456"), Row.of(Math.PI, "ABC", Arrays.asList(1L, 2L, 3L))),
			Row.of(3.1f, "DEF", LocalDateTime.parse("2000-12-12T12:30:57.1234567"), Row.of(3.1f, "DEF", Arrays.asList(1D, 2D, 3D))),
			Row.of(99L, "DEFG", LocalDateTime.parse("2000-12-12T12:30:57.12345678"), Row.of(99L, "DEFG", Arrays.asList(1f, 2f, 3f))),
			Row.of(0d, "D", LocalDateTime.parse("2000-12-12T12:30:57.123"), Row.of(0d, "D", Arrays.asList(1, 2, 3)))
		);

		DataType rowType = DataTypes.ROW(
			DataTypes.FIELD("a", TypeConversions.fromLegacyInfoToDataType(Types.BIG_DEC)),
			DataTypes.FIELD("b", TypeConversions.fromLegacyInfoToDataType(Types.STRING)),
			DataTypes.FIELD("c", TypeConversions.fromLegacyInfoToDataType(Types.SQL_TIMESTAMP)),
			DataTypes.FIELD(
				"row",
				DataTypes.ROW(
					DataTypes.FIELD("a", TypeConversions.fromLegacyInfoToDataType(Types.BIG_DEC)),
					DataTypes.FIELD("c", TypeConversions.fromLegacyInfoToDataType(Types.STRING)),
					DataTypes.FIELD("d", TypeConversions.fromLegacyInfoToDataType(Types.OBJECT_ARRAY(Types.BIG_DEC))))
			)
		);

		StreamExecutionEnvironment streamExecEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useOldPlanner().build();
		StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(
				streamExecEnvironment, settings);

		Table t = tableEnvironment.fromValues(
			rowType,
			data
		);
		DataStream<Row> rowDataStream = tableEnvironment.toAppendStream(t, Row.class);
		StreamITCase.clear();
		rowDataStream.addSink(new StreamITCase.StringSink<>());

		streamExecEnvironment.execute();

		List<String> expected = Arrays.asList(
			"0,D,2000-12-12 12:30:57.123,0,D,[1, 2, 3]",
			"1,ABC,2000-12-12 12:30:57.12,1,ABC,[1, 2, 3]",
			"3.0999999046325684,DEF,2000-12-12 12:30:57.123,3.0999999046325684,DEF,[1.0, 2.0, 3.0]",
			"3.141592653589793,ABC,2000-12-12 12:30:57.123,3.141592653589793,ABC,[1, 2, 3]",
			"99,DEFG,2000-12-12 12:30:57.123,99,DEFG,[1.0, 2.0, 3.0]"
		);
		StreamITCase.compareWithList(expected);
	}
}
