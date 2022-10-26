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

import org.junit.jupiter.api.Test;

import java.time.DateTimeException;
import java.time.Duration;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TableConfig}. */
public class TableConfigTest {

    private static final TableConfig CONFIG_BY_METHOD = TableConfig.getDefault();
    private static final TableConfig CONFIG_BY_CONFIGURATION = TableConfig.getDefault();
    private static final Configuration configuration = new Configuration();

    @Test
    void testSetAndGetSqlDialect() {
        configuration.setString("table.sql-dialect", "HIVE");
        CONFIG_BY_CONFIGURATION.addConfiguration(configuration);
        CONFIG_BY_METHOD.setSqlDialect(SqlDialect.HIVE);

        assertThat(CONFIG_BY_METHOD.getSqlDialect()).isEqualTo(SqlDialect.HIVE);
        assertThat(CONFIG_BY_CONFIGURATION.getSqlDialect()).isEqualTo(SqlDialect.HIVE);
    }

    @Test
    void testSetAndGetMaxGeneratedCodeLength() {
        configuration.setString("table.generated-code.max-length", "5000");
        CONFIG_BY_CONFIGURATION.addConfiguration(configuration);
        CONFIG_BY_METHOD.setMaxGeneratedCodeLength(5000);

        assertThat(CONFIG_BY_METHOD.getMaxGeneratedCodeLength()).isEqualTo(Integer.valueOf(5000));
        assertThat(CONFIG_BY_CONFIGURATION.getMaxGeneratedCodeLength())
                .isEqualTo(Integer.valueOf(5000));
    }

    @Test
    void testSetAndGetLocalTimeZone() {
        configuration.setString("table.local-time-zone", "Asia/Shanghai");
        CONFIG_BY_CONFIGURATION.addConfiguration(configuration);
        CONFIG_BY_METHOD.setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        assertThat(CONFIG_BY_METHOD.getLocalTimeZone()).isEqualTo(ZoneId.of("Asia/Shanghai"));
        assertThat(CONFIG_BY_CONFIGURATION.getLocalTimeZone())
                .isEqualTo(ZoneId.of("Asia/Shanghai"));

        configuration.setString("table.local-time-zone", "GMT-08:00");
        CONFIG_BY_CONFIGURATION.addConfiguration(configuration);
        CONFIG_BY_METHOD.setLocalTimeZone(ZoneId.of("GMT-08:00"));

        assertThat(CONFIG_BY_METHOD.getLocalTimeZone()).isEqualTo(ZoneId.of("GMT-08:00"));
        assertThat(CONFIG_BY_CONFIGURATION.getLocalTimeZone()).isEqualTo(ZoneId.of("GMT-08:00"));
    }

    @Test
    public void testSetInvalidLocalTimeZone() {
        assertThatThrownBy(() -> CONFIG_BY_METHOD.setLocalTimeZone(ZoneId.of("UTC-10:00")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The supported Zone ID is either a full name such as 'America/Los_Angeles',"
                                + " or a custom timezone id such as 'GMT-08:00', but configured Zone ID is 'UTC-10:00'.");
    }

    @Test
    public void testInvalidGmtLocalTimeZone() {
        assertThatThrownBy(() -> CONFIG_BY_METHOD.setLocalTimeZone(ZoneId.of("GMT-8:00")))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid ID for offset-based ZoneId: GMT-8:00");
    }

    @Test
    void testGetInvalidLocalTimeZone() {
        configuration.setString("table.local-time-zone", "UTC+8");
        CONFIG_BY_CONFIGURATION.addConfiguration(configuration);
        assertThatThrownBy(CONFIG_BY_CONFIGURATION::getLocalTimeZone)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The supported Zone ID is either a full name such as 'America/Los_Angeles',"
                                + " or a custom timezone id such as 'GMT-08:00', but configured Zone ID is 'UTC+8'.");
    }

    @Test
    void testGetInvalidAbbreviationLocalTimeZone() {
        configuration.setString("table.local-time-zone", "PST");
        CONFIG_BY_CONFIGURATION.addConfiguration(configuration);
        assertThatThrownBy(CONFIG_BY_CONFIGURATION::getLocalTimeZone)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The supported Zone ID is either a full name such as 'America/Los_Angeles',"
                                + " or a custom timezone id such as 'GMT-08:00', but configured Zone ID is 'PST'.");
    }

    @Test
    void testSetAndGetIdleStateRetention() {
        configuration.setString("table.exec.state.ttl", "1 h");
        CONFIG_BY_CONFIGURATION.addConfiguration(configuration);
        CONFIG_BY_METHOD.setIdleStateRetention(Duration.ofHours(1));

        assertThat(CONFIG_BY_METHOD.getIdleStateRetention()).isEqualTo(Duration.ofHours(1));
        assertThat(CONFIG_BY_CONFIGURATION.getIdleStateRetention()).isEqualTo(Duration.ofHours(1));
    }
}
