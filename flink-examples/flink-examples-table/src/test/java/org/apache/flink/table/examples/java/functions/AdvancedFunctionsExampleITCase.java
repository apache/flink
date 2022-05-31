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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for Java {@link AdvancedFunctionsExample}. */
class AdvancedFunctionsExampleITCase extends ExampleOutputTestBase {

    @Test
    void testExample() throws Exception {
        AdvancedFunctionsExample.main(new String[0]);
        final String consoleOutput = getOutputString();

        testExecuteLastDatedValueFunction(consoleOutput);
        testExecuteInternalRowMergerFunction(consoleOutput);
    }

    private void testExecuteLastDatedValueFunction(String consoleOutput) {
        assertThat(consoleOutput)
                .contains("|                Guillermo Smith |                (5, 2020-12-05) |")
                .contains("|                    John Turner |               (12, 2020-10-02) |")
                .contains("|                 Brandy Sanders |                (1, 2020-10-14) |")
                .contains("|                Valeria Mendoza |               (10, 2020-06-02) |")
                .contains("|                   Ellen Ortega |              (100, 2020-06-18) |")
                .contains("|                 Leann Holloway |                (9, 2020-05-26) |");
    }

    private void testExecuteInternalRowMergerFunction(String consoleOutput) {
        assertThat(consoleOutput)
                .contains("|                Guillermo Smith | (1992-12-12, New Jersey, 81... |")
                .contains("|                Valeria Mendoza | (1970-03-28, Los Angeles, 9... |")
                .contains("|                 Leann Holloway | (1989-05-21, Eugene, 614-88... |");
    }
}
