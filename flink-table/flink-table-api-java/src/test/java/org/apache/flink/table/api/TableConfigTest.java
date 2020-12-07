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

package org.apache.flink.table.api;

import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link TableConfig}.
 */
public class TableConfigTest {
	private static TableConfig configByMethod = new TableConfig();
	private static TableConfig configByConfiguration = new TableConfig();
	private static Configuration configuration = new Configuration();

	@Test
	public void testSetAndGetSqlDialect() {
		configuration.setString("table.sql-dialect", "HIVE");
		configByConfiguration.addConfiguration(configuration);
		configByMethod.setSqlDialect(SqlDialect.HIVE);

		assertEquals(SqlDialect.HIVE, configByMethod.getSqlDialect());
		assertEquals(SqlDialect.HIVE, configByConfiguration.getSqlDialect());
	}

	@Test
	public void testSetAndGetMaxGeneratedCodeLength() {
		configuration.setString("table.generated-code.max-length", "5000");
		configByConfiguration.addConfiguration(configuration);
		configByMethod.setMaxGeneratedCodeLength(5000);

		assertEquals(Integer.valueOf(5000), configByMethod.getMaxGeneratedCodeLength());
		assertEquals(Integer.valueOf(5000), configByConfiguration.getMaxGeneratedCodeLength());
	}

	@Test
	public void testSetAndGetLocalTimeZone() {
		configuration.setString("table.local-time-zone", "Asia/Shanghai");
		configByConfiguration.addConfiguration(configuration);
		configByMethod.setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

		assertEquals(ZoneId.of("Asia/Shanghai"), configByMethod.getLocalTimeZone());
		assertEquals(ZoneId.of("Asia/Shanghai"), configByConfiguration.getLocalTimeZone());
	}

	@Test
	public void testSetAndGetIdleStateRetention() {
		configuration.setString("table.exec.state.ttl", "1 h");
		configByConfiguration.addConfiguration(configuration);
		configByMethod.setIdleStateRetention(Duration.ofHours(1));

		assertEquals(Duration.ofHours(1), configByMethod.getIdleStateRetention());
		assertEquals(Duration.ofHours(1), configByConfiguration.getIdleStateRetention());
	}
}
