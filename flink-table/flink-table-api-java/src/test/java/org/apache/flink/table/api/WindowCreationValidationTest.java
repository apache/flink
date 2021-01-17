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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Test failures for the creation of window. */
public class WindowCreationValidationTest {

    @Rule public final ExpectedException exception = ExpectedException.none();

    @Test
    public void testTumbleOverForString() {
        exception.expect(TableException.class);
        exception.expectMessage("Construction of PlannerExpressionParserImpl class failed.");
        Tumble.over("4.hours");
    }

    @Test
    public void testSlideOverForString() {
        exception.expect(TableException.class);
        exception.expectMessage("Construction of PlannerExpressionParserImpl class failed.");
        Slide.over("4.hours");
    }

    @Test
    public void testSessionWithGapForString() {
        exception.expect(TableException.class);
        exception.expectMessage("Construction of PlannerExpressionParserImpl class failed.");
        Session.withGap("4.hours");
    }

    @Test
    public void testOverWithPartitionByForString() {
        exception.expect(TableException.class);
        exception.expectMessage("Construction of PlannerExpressionParserImpl class failed.");
        Over.partitionBy("a");
    }
}
