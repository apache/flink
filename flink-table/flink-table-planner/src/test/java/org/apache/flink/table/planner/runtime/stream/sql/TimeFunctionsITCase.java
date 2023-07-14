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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.delegation.ClockPlannerConfig;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for evaluating time functions. */
public class TimeFunctionsITCase {

    @Test
    void testTemporalFunctionsInStreamModeQueryTime() throws Exception {
        final TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        env.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));
        env.getConfig()
                .set(
                        ExecutionConfigOptions.TIME_FUNCTION_EVALUATION,
                        ExecutionConfigOptions.TimeFunctionEvaluation.QUERY_START);
        final ManualClockPlannerConfig clockPlannerConfig = new ManualClockPlannerConfig();
        env.getConfig().setPlannerConfig(clockPlannerConfig);

        try (CloseableIterator<Row> iterator =
                env.executeSql(
                                "SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP,NOW(),LOCALTIME,LOCALTIMESTAMP")
                        .collect()) {
            final Row row = iterator.next();

            assertThat(row.toString())
                    .isEqualTo(
                            "+I[1970-01-01, 01:00:01.234, "
                                    + "1970-01-01T00:00:01.234Z, 1970-01-01T00:00:01.234Z, 01:00:01.234, "
                                    + "1970-01-01T01:00:01.234]");
        }
    }

    private static class ManualClockPlannerConfig implements ClockPlannerConfig {

        private final ManualClock clock = new ManualClock(1234L * 1_000_000);

        @Override
        public Clock getClock() {
            return clock;
        }
    }
}
