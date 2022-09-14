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

package org.apache.flink.graph.drivers;

import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Base class for drivers requiring the key ID to implement {@code CopyableValue}. This class
 * overrides {@link DriverBaseITCase} to restrict the tested ID types.
 */
public abstract class CopyableValueDriverBaseITCase extends DriverBaseITCase {

    protected CopyableValueDriverBaseITCase(String idType, TestExecutionMode mode) {
        super(idType, mode);
    }

    // limit tests to types supporting CopyableValue
    @Parameterized.Parameters(name = "ID type = {0}, Execution mode = {1}")
    public static Collection<Object[]> executionModes() {
        List<Object[]> executionModes = new ArrayList<>();

        for (String idType :
                new String[] {
                    "byte", "short", "char", "integer", "long", "float", "double", "string"
                }) {
            for (TestExecutionMode executionMode : TestExecutionMode.values()) {
                executionModes.add(new Object[] {idType, executionMode});
            }
        }

        return executionModes;
    }
}
