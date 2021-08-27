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

package org.apache.flink.table.examples.java.functions;

import org.apache.flink.table.examples.utils.ExampleOutputTestBase;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

/** Test for Java {@link AdvancedFunctionsExample}. */
public class AdvancedFunctionsExampleITCase extends ExampleOutputTestBase {

    @Test
    public void testExample() throws Exception {
        AdvancedFunctionsExample.main(new String[0]);
        final String consoleOutput = getOutputString();

        testExecuteLastDatedValueFunction(consoleOutput);
        testExecuteInternalRowMergerFunction(consoleOutput);
    }

    private void testExecuteLastDatedValueFunction(String consoleOutput) {
        assertThat(
                consoleOutput,
                containsString(
                        "|                Guillermo Smith |              +I[5, 2020-12-05] |"));
        assertThat(
                consoleOutput,
                containsString(
                        "|                    John Turner |             +I[12, 2020-10-02] |"));
        assertThat(
                consoleOutput,
                containsString(
                        "|                 Brandy Sanders |              +I[1, 2020-10-14] |"));
        assertThat(
                consoleOutput,
                containsString(
                        "|                Valeria Mendoza |             +I[10, 2020-06-02] |"));
        assertThat(
                consoleOutput,
                containsString(
                        "|                   Ellen Ortega |            +I[100, 2020-06-18] |"));
        assertThat(
                consoleOutput,
                containsString(
                        "|                 Leann Holloway |              +I[9, 2020-05-26] |"));
    }

    private void testExecuteInternalRowMergerFunction(String consoleOutput) {
        assertThat(
                consoleOutput,
                containsString(
                        "|                Guillermo Smith | +I[1992-12-12, New Jersey, ... |"));
        assertThat(
                consoleOutput,
                containsString(
                        "|                Valeria Mendoza | +I[1970-03-28, Los Angeles,... |"));
        assertThat(
                consoleOutput,
                containsString(
                        "|                 Leann Holloway | +I[1989-05-21, Eugene, 614-... |"));
    }
}
