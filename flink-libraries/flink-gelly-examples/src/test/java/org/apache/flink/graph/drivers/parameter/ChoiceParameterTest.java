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
import org.apache.flink.client.program.ProgramParametrizationException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link ChoiceParameter}. */
public class ChoiceParameterTest extends ParameterTestBase {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    private ChoiceParameter parameter;

    @Before
    public void setup() {
        super.setup();
        parameter = new ChoiceParameter(owner, "choice");
    }

    // With default

    @Test
    public void testWithDefaultWithParameter() {
        parameter.setDefaultValue("default").addChoices("c0", "c1", "c2");
        Assert.assertEquals("[--choice <default | c0 | c1 | c2>]", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {"--choice", "c1"}));
        Assert.assertEquals("c1", parameter.getValue());
    }

    @Test
    public void testWithDefaultWithoutParameter() {
        parameter.setDefaultValue("default").addChoices("c0", "c1", "c2");
        Assert.assertEquals("[--choice <default | c0 | c1 | c2>]", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {}));
        Assert.assertEquals("default", parameter.getValue());
    }

    // Without default

    @Test
    public void testWithoutDefaultWithParameter() {
        parameter.addChoices("c0", "c1", "c2");
        Assert.assertEquals("--choice <c0 | c1 | c2>", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {"--choice", "c2"}));
        Assert.assertEquals("c2", parameter.getValue());
    }

    @Test
    public void testWithoutDefaultWithoutParameter() {
        parameter.addChoices("c0", "c1", "c2");
        Assert.assertEquals("--choice <c0 | c1 | c2>", parameter.getUsage());

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("Must select a choice for option 'choice': '[c0, c1, c2]'");

        parameter.configure(ParameterTool.fromArgs(new String[] {}));
    }
}
