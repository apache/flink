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

package org.apache.flink.graph.drivers.parameter;

import org.apache.flink.api.java.utils.ParameterTool;
import org.junit.Before;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link BooleanParameter}. */
public class BooleanParameterTest extends ParameterTestBase {

    private BooleanParameter parameter;

    @Before
    public void setup() {
        super.setup();

        parameter = new BooleanParameter(owner, "test");
    }

    @Test
    public void testTrue() {
        Assertions.assertEquals("[--test]", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test"}));
        Assertions.assertEquals(true, parameter.getValue());
    }

    @Test
    public void testFalse() {
        Assertions.assertEquals("[--test]", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {}));
        Assertions.assertEquals(false, parameter.getValue());
    }
}
