/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.openai;

import org.apache.flink.configuration.ConfigurationUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for utility and common classes related to Bailian model. */
public class ContextOverflowActionTest {
    @Test
    public void testMaxTokens() {
        testMaxTokens(2, "truncated-tail", "apple banana cherry", "apple banana");
        testMaxTokens(2, "truncated-tail-log", "apple banana cherry", "apple banana");
        testMaxTokens(2, "truncated-head", "apple banana cherry", " banana cherry");
        testMaxTokens(2, "truncated-head-log", "apple banana cherry", " banana cherry");
        testMaxTokens(2, "skipped", "apple banana cherry", null);
        testMaxTokens(2, "skipped-log", "apple banana cherry", null);
    }

    private void testMaxTokens(
            Integer maxTokens, String contextOverflowAction, String input, String expectedOutput) {
        ContextOverflowAction action =
                ConfigurationUtils.convertToEnum(
                        contextOverflowAction, ContextOverflowAction.class);
        action.initializeEncodingForContextLimit("gpt-4", maxTokens);
        assertThat(action.processTokensWithLimit("gpt-4", input, maxTokens))
                .isEqualTo(expectedOutput);
    }
}
