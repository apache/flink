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

package org.apache.flink.optimizer.plandump;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NumberFormattingTest {

    @Test
    void testFormatNumberNoDigit() {
        assertThat("0.0").isEqualTo(PlanJSONDumpGenerator.formatNumber(0));
        assertThat("0.00").isEqualTo(PlanJSONDumpGenerator.formatNumber(0.0000000001));
        assertThat("-1.0").isEqualTo(PlanJSONDumpGenerator.formatNumber(-1.0));
        assertThat("1.00").isEqualTo(PlanJSONDumpGenerator.formatNumber(1));
        assertThat("17.00").isEqualTo(PlanJSONDumpGenerator.formatNumber(17));
        assertThat("17.44").isEqualTo(PlanJSONDumpGenerator.formatNumber(17.44));
        assertThat("143.00").isEqualTo(PlanJSONDumpGenerator.formatNumber(143));
        assertThat("143.40").isEqualTo(PlanJSONDumpGenerator.formatNumber(143.4));
        assertThat("143.50").isEqualTo(PlanJSONDumpGenerator.formatNumber(143.5));
        assertThat("143.60").isEqualTo(PlanJSONDumpGenerator.formatNumber(143.6));
        assertThat("143.45").isEqualTo(PlanJSONDumpGenerator.formatNumber(143.45));
        assertThat("143.55").isEqualTo(PlanJSONDumpGenerator.formatNumber(143.55));
        assertThat("143.65").isEqualTo(PlanJSONDumpGenerator.formatNumber(143.65));
        assertThat("143.66").isEqualTo(PlanJSONDumpGenerator.formatNumber(143.655));

        assertThat("1.13 K").isEqualTo(PlanJSONDumpGenerator.formatNumber(1126.0));
        assertThat("11.13 K").isEqualTo(PlanJSONDumpGenerator.formatNumber(11126.0));
        assertThat("118.13 K").isEqualTo(PlanJSONDumpGenerator.formatNumber(118126.0));

        assertThat("1.44 M").isEqualTo(PlanJSONDumpGenerator.formatNumber(1435126.0));
    }
}
