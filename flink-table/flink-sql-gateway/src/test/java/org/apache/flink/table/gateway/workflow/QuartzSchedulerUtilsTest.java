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

package org.apache.flink.table.gateway.workflow;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils;

import org.junit.jupiter.api.Test;
import org.quartz.JobKey;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.QUARTZ_JOB_GROUP;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.QUARTZ_JOB_PREFIX;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.dateToString;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.fromJson;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.getJobKey;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.toJson;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link QuartzSchedulerUtils}. */
public class QuartzSchedulerUtilsTest {

    @Test
    void testJsonSerDe() {
        String identifier = ObjectIdentifier.of("a", "b", "c").asSerializableString();
        Map<String, String> dynamicOptions = new HashMap<>();
        dynamicOptions.put("k1", "v1");
        dynamicOptions.put("k2", "v2");

        Map<String, String> initConfig = new HashMap<>();
        initConfig.put("k3", "v4");
        initConfig.put("k4", "v5");

        Map<String, String> executionConfig = new HashMap<>();
        executionConfig.put("k5", "v5");
        executionConfig.put("k6", "v6");

        String url = "http://localhost:8083";
        WorkflowInfo expected =
                new WorkflowInfo(identifier, dynamicOptions, initConfig, executionConfig, url);

        WorkflowInfo serde = fromJson(toJson(expected), WorkflowInfo.class);
        assertThat(serde).isEqualTo(expected);
    }

    @Test
    void testConvertDateToString() {
        LocalDateTime localDateTime = LocalDateTime.of(2024, 5, 22, 12, 30, 15);
        Date date = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
        String actual = dateToString(date);

        String expected = "2024-05-22 12:30:15";
        assertThat(actual).isEqualTo(expected);
    }

    @Test
    void testGetJobKey() {
        String materializedTableIdentifier = "`a`.`b`.`c`";
        JobKey actual = getJobKey(materializedTableIdentifier);

        assertThat(actual.getName())
                .isEqualTo(QUARTZ_JOB_PREFIX + "_" + materializedTableIdentifier);
        assertThat(actual.getGroup()).isEqualTo(QUARTZ_JOB_GROUP);
    }
}
