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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Duration;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

/** Tests for {@link TableConfig}. */
public class TableConfigTest {

    @Rule public ExpectedException expectedException = ExpectedException.none();

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

        configuration.setString("table.local-time-zone", "GMT-08:00");
        configByConfiguration.addConfiguration(configuration);
        configByMethod.setLocalTimeZone(ZoneId.of("GMT-08:00"));

        assertEquals(ZoneId.of("GMT-08:00"), configByMethod.getLocalTimeZone());
        assertEquals(ZoneId.of("GMT-08:00"), configByConfiguration.getLocalTimeZone());
    }

    @Test
    public void testSetInvalidLocalTimeZone() {
        expectedException.expectMessage(
                "The supported Zone ID is either a full name such as 'America/Los_Angeles',"
                        + " or a custom timezone id such as 'GMT-08:00', but configured Zone ID is 'UTC-10:00'.");
        configByMethod.setLocalTimeZone(ZoneId.of("UTC-10:00"));
    }

    @Test
    public void testInvalidGmtLocalTimeZone() {
        expectedException.expectMessage("Invalid ID for offset-based ZoneId: GMT-8:00");
        configByMethod.setLocalTimeZone(ZoneId.of("GMT-8:00"));
    }

    @Test
    public void testGetInvalidLocalTimeZone() {
        configuration.setString("table.local-time-zone", "UTC+8");
        configByConfiguration.addConfiguration(configuration);
        expectedException.expectMessage(
                "The supported Zone ID is either a full name such as 'America/Los_Angeles',"
                        + " or a custom timezone id such as 'GMT-08:00', but configured Zone ID is 'UTC+8'.");
        configByConfiguration.getLocalTimeZone();
    }

    @Test
    public void testGetInvalidAbbreviationLocalTimeZone() {
        configuration.setString("table.local-time-zone", "PST");
        configByConfiguration.addConfiguration(configuration);
        expectedException.expectMessage(
                "The supported Zone ID is either a full name such as 'America/Los_Angeles',"
                        + " or a custom timezone id such as 'GMT-08:00', but configured Zone ID is 'PST'.");
        configByConfiguration.getLocalTimeZone();
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
