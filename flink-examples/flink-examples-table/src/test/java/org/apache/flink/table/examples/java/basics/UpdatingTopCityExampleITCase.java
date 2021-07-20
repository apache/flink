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

import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;

/** Test for {@link UpdatingTopCityExample}. */
public class UpdatingTopCityExampleITCase extends ExampleOutputTestBase {

    @Test
    public void testExample() throws Exception {
        UpdatingTopCityExample.main(new String[0]);
        final String consoleOutput = getOutputString();
        assertThat(consoleOutput, containsString("AZ, Phoenix, 2015, 4581120"));
        assertThat(consoleOutput, containsString("IL, Chicago, 2015, 9557880"));
        assertThat(consoleOutput, containsString("CA, San Francisco, 2015, 4649540"));
        assertThat(consoleOutput, containsString("CA, Los Angeles, 2015, 13251000"));
        assertThat(consoleOutput, containsString("TX, Dallas, 2015, 7109280"));
        assertThat(consoleOutput, containsString("TX, Houston, 2015, 6676560"));
    }
}
