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

package org.apache.flink.table.types.inference;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import static org.apache.flink.table.types.inference.TypeTransformations.timeToSqlTypes;
import static org.junit.Assert.assertEquals;

/**
 * Tests for built-in {@link TypeTransformations}.
 */
public class TypeTransformationsTest {

	@Test
	public void testTimeToSqlTypes() {
		DataType dataType = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING()),
			DataTypes.FIELD("b", DataTypes.TIMESTAMP()),
			DataTypes.FIELD("c", DataTypes.TIMESTAMP(5)),
			DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.TIME())),
			DataTypes.FIELD("e", DataTypes.MAP(DataTypes.DATE(), DataTypes.TIME(9))),
			DataTypes.FIELD("f", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
		);

		DataType expected = DataTypes.ROW(
			DataTypes.FIELD("a", DataTypes.STRING()),
			DataTypes.FIELD("b", DataTypes.TIMESTAMP().bridgedTo(Timestamp.class)),
			DataTypes.FIELD("c", DataTypes.TIMESTAMP(5).bridgedTo(Timestamp.class)),
			DataTypes.FIELD("d", DataTypes.ARRAY(DataTypes.TIME().bridgedTo(Time.class))),
			DataTypes.FIELD("e", DataTypes.MAP(
				DataTypes.DATE().bridgedTo(Date.class),
				DataTypes.TIME(9).bridgedTo(Time.class))),
			DataTypes.FIELD("f", DataTypes.TIMESTAMP_WITH_TIME_ZONE())
		);

		assertEquals(expected, DataTypeUtils.transform(dataType, timeToSqlTypes()));
	}

}
