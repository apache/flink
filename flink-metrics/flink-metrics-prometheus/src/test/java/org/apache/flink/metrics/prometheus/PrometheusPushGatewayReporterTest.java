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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

/** Test for {@link PrometheusPushGatewayReporter}. */
public class PrometheusPushGatewayReporterTest extends TestLogger {

    @Test
    public void testParseGroupingKey() {
        PrometheusPushGatewayReporter reporter = new PrometheusPushGatewayReporter();
        Map<String, String> groupingKey = reporter.parseGroupingKey("k1=v1;k2=v2");
        Assertions.assertNotNull(groupingKey);
        Assertions.assertEquals("v1", groupingKey.get("k1"));
        Assertions.assertEquals("v2", groupingKey.get("k2"));
    }

    @Test
    public void testParseIncompleteGroupingKey() {
        PrometheusPushGatewayReporter reporter = new PrometheusPushGatewayReporter();
        Map<String, String> groupingKey = reporter.parseGroupingKey("k1=");
        Assertions.assertTrue(groupingKey.isEmpty());

        groupingKey = reporter.parseGroupingKey("=v1");
        Assertions.assertTrue(groupingKey.isEmpty());

        groupingKey = reporter.parseGroupingKey("k1");
        Assertions.assertTrue(groupingKey.isEmpty());
    }
}
