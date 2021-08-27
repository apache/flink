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

/** Tests for {@link DoubleParameter}. */
public class DoubleParameterTest extends ParameterTestBase {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    private DoubleParameter parameter;

    @Before
    public void setup() {
        super.setup();

        parameter = new DoubleParameter(owner, "test");
    }

    // Test configuration

    @Test
    public void testDefaultValueBelowMinimum() {
        parameter.setMinimumValue(1.0, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("Default value (0.0) must be greater than minimum (1.0)");

        parameter.setDefaultValue(0.0);
    }

    @Test
    public void testDefaultValueBetweenMinAndMax() {
        parameter.setMinimumValue(-1.0, false);
        parameter.setMaximumValue(1.0, false);

        parameter.setDefaultValue(0);
    }

    @Test
    public void testDefaultValueAboveMaximum() {
        parameter.setMaximumValue(-1.0, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("Default value (0.0) must be less than maximum (-1.0)");

        parameter.setDefaultValue(0);
    }

    @Test
    public void testMinimumValueAboveMaximum() {
        parameter.setMaximumValue(0.0, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("Minimum value (1.0) must be less than maximum (0.0)");

        parameter.setMinimumValue(1.0, false);
    }

    @Test
    public void testMinimumValueAboveDefault() {
        parameter.setDefaultValue(0);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("Minimum value (1.0) must be less than default (0.0)");

        parameter.setMinimumValue(1.0, false);
    }

    @Test
    public void testMaximumValueBelowMinimum() {
        parameter.setMinimumValue(0.0, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("Maximum value (-1.0) must be greater than minimum (0.0)");

        parameter.setMaximumValue(-1.0, false);
    }

    @Test
    public void testMaximumValueBelowDefault() {
        parameter.setDefaultValue(0);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("Maximum value (-1.0) must be greater than default (0.0)");

        parameter.setMaximumValue(-1.0, false);
    }

    @Test
    public void testEqualMinimumAndMaximumInclusive() {
        parameter.setMinimumValue(0.0, true);
        parameter.setMaximumValue(0.0, true);
    }

    @Test
    public void testMinimumEqualsMaximumExclusive() {
        parameter.setMaximumValue(0.0, true);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("Minimum value (0.0) must be less than maximum (0.0)");

        parameter.setMinimumValue(0.0, false);
    }

    @Test
    public void testMaximumEqualsMinimumExclusive() {
        parameter.setMinimumValue(0.0, true);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("Maximum value (0.0) must be greater than minimum (0.0)");

        parameter.setMaximumValue(0.0, false);
    }

    // With default

    @Test
    public void testWithDefaultWithParameter() {
        parameter.setDefaultValue(43.21);
        Assert.assertEquals("[--test TEST]", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "12.34"}));
        Assert.assertEquals(new Double(12.34), parameter.getValue());
    }

    @Test
    public void testWithDefaultWithoutParameter() {
        parameter.setDefaultValue(43.21);
        Assert.assertEquals("[--test TEST]", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {}));
        Assert.assertEquals(new Double(43.21), parameter.getValue());
    }

    // Without default

    @Test
    public void testWithoutDefaultWithParameter() {
        Assert.assertEquals("--test TEST", parameter.getUsage());

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "12.34"}));
        Assert.assertEquals(new Double(12.34), parameter.getValue());
    }

    @Test
    public void testWithoutDefaultWithoutParameter() {
        Assert.assertEquals("--test TEST", parameter.getUsage());

        expectedException.expect(RuntimeException.class);
        expectedException.expectMessage("No data for required key 'test'");

        parameter.configure(ParameterTool.fromArgs(new String[] {}));
    }

    // Min

    @Test
    public void testMinInRange() {
        parameter.setMinimumValue(0, false);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "1"}));
        Assert.assertEquals(new Double(1), parameter.getValue());
    }

    @Test
    public void testMinAtRangeInclusive() {
        parameter.setMinimumValue(0, true);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "0"}));
        Assert.assertEquals(new Double(0), parameter.getValue());
    }

    @Test
    public void testMinAtRangeExclusive() {
        parameter.setMinimumValue(0, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be greater than 0.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "0"}));
    }

    @Test
    public void testMinOutOfRange() {
        parameter.setMinimumValue(0, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be greater than 0.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "-1"}));
    }

    @Test
    public void testMinOutOfRangeExclusive() {
        parameter.setMinimumValue(0, true);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be greater than or equal to 0.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "-1"}));
    }

    // Max

    @Test
    public void testMaxInRange() {
        parameter.setMaximumValue(0, false);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "-1"}));
        Assert.assertEquals(new Double(-1), parameter.getValue());
    }

    @Test
    public void testMaxAtRangeInclusive() {
        parameter.setMaximumValue(0, true);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "0"}));
        Assert.assertEquals(new Double(0), parameter.getValue());
    }

    @Test
    public void testMaxAtRangeExclusive() {
        parameter.setMaximumValue(0, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be less than 0.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "0"}));
    }

    @Test
    public void testMaxOutOfRange() {
        parameter.setMaximumValue(0, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be less than 0.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "1"}));
    }

    @Test
    public void testMaxOutOfRangeExclusive() {
        parameter.setMaximumValue(0, true);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be less than or equal to 0.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "1"}));
    }

    // Min and max

    @Test
    public void testMinAndMaxBelowRange() {
        parameter.setMinimumValue(-1, false);
        parameter.setMaximumValue(1, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be greater than -1.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "-2"}));
    }

    @Test
    public void testMinAndMaxAtRangeMinimumExclusive() {
        parameter.setMinimumValue(-1, false);
        parameter.setMaximumValue(1, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be greater than -1.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "-1"}));
    }

    @Test
    public void testMinAndMaxAtRangeMinimumInclusive() {
        parameter.setMinimumValue(-1, true);
        parameter.setMaximumValue(1, true);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "-1"}));
        Assert.assertEquals(new Double(-1), parameter.getValue());
    }

    @Test
    public void testMinAndMaxInRange() {
        parameter.setMinimumValue(-1, false);
        parameter.setMaximumValue(1, false);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "0"}));
        Assert.assertEquals(new Double(0), parameter.getValue());
    }

    @Test
    public void testMinAndMaxAtRangeMaximumInclusive() {
        parameter.setMinimumValue(-1, true);
        parameter.setMaximumValue(1, true);
        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "1"}));
        Assert.assertEquals(new Double(1), parameter.getValue());
    }

    @Test
    public void testMinAndMaxAtRangeMaximumExclusive() {
        parameter.setMinimumValue(-1, false);
        parameter.setMaximumValue(1, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be less than 1.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "1"}));
    }

    @Test
    public void testMinAndMaxAboveRange() {
        parameter.setMinimumValue(-1, false);
        parameter.setMaximumValue(1, false);

        expectedException.expect(ProgramParametrizationException.class);
        expectedException.expectMessage("test must be less than 1.0");

        parameter.configure(ParameterTool.fromArgs(new String[] {"--test", "2"}));
    }
}
