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

package org.apache.flink.table.examples.java.basics;

import org.apache.flink.table.examples.utils.ExampleOutputTestBase;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link UpdatingTopCityExample}. */
class UpdatingTopCityExampleITCase extends ExampleOutputTestBase {

    @Test
    void testExample() throws Exception {
        UpdatingTopCityExample.main(new String[0]);
        final String consoleOutput = getOutputString();
        assertThat(consoleOutput)
                .contains("AZ, Phoenix, 2015, 4581120")
                .contains("IL, Chicago, 2015, 9557880")
                .contains("CA, San Francisco, 2015, 4649540")
                .contains("CA, Los Angeles, 2015, 13251000")
                .contains("TX, Dallas, 2015, 7109280")
                .contains("TX, Houston, 2015, 6676560");
    }
}
