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

package org.apache.flink.table.examples.utils;

import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

/** Test base for validating the example output to stdout. */
public abstract class ExampleOutputTestBase {

    @ClassRule
    public static MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    private PrintStream originalPrintStream;

    private ByteArrayOutputStream testOutputStream;

    @Before
    public void init() {
        originalPrintStream = System.out;
        testOutputStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(testOutputStream));
    }

    protected String getOutputString() {
        return testOutputStream.toString();
    }

    @After
    public void finalize() {
        System.setOut(originalPrintStream);
    }
}
