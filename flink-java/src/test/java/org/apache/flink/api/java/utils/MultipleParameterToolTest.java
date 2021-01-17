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

package org.apache.flink.api.java.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/** Tests for {@link MultipleParameterTool}. */
public class MultipleParameterToolTest extends AbstractParameterToolTest {

    @Test
    public void testFromCliArgsWithMultipleParameters() {
        MultipleParameterTool parameter =
                (MultipleParameterTool)
                        createParameterToolFromArgs(
                                new String[] {
                                    "--input",
                                    "myInput",
                                    "-expectedCount",
                                    "15",
                                    "--multi",
                                    "multiValue1",
                                    "--multi",
                                    "multiValue2",
                                    "--withoutValues",
                                    "--negativeFloat",
                                    "-0.58",
                                    "-isWorking",
                                    "true",
                                    "--maxByte",
                                    "127",
                                    "-negativeShort",
                                    "-1024"
                                });

        Assert.assertEquals(8, parameter.getNumberOfParameters());
        validate(parameter);
        Assert.assertTrue(parameter.has("withoutValues"));
        Assert.assertEquals(-0.58, parameter.getFloat("negativeFloat"), 0.1);
        Assert.assertTrue(parameter.getBoolean("isWorking"));
        Assert.assertEquals(127, parameter.getByte("maxByte"));
        Assert.assertEquals(-1024, parameter.getShort("negativeShort"));

        exception.expect(IllegalStateException.class);
        exception.expectMessage("Key multi should has only one value");
        parameter.get("multi");
    }

    @Test
    public void testUnrequestedMultiParameter() {
        MultipleParameterTool parameter =
                (MultipleParameterTool)
                        createParameterToolFromArgs(
                                new String[] {"--multi", "v1", "--multi", "v2", "--multi2", "vv1"});
        Assert.assertEquals(createHashSet("multi", "multi2"), parameter.getUnrequestedParameters());

        Assert.assertEquals(Arrays.asList("v1", "v2"), parameter.getMultiParameter("multi"));
        Assert.assertEquals(createHashSet("multi2"), parameter.getUnrequestedParameters());

        Assert.assertEquals(
                Collections.singletonList("vv1"), parameter.getMultiParameterRequired("multi2"));
        Assert.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testMerged() {
        MultipleParameterTool parameter1 =
                (MultipleParameterTool)
                        createParameterToolFromArgs(
                                new String[] {
                                    "--input", "myInput", "--merge", "v1", "--merge", "v2"
                                });
        MultipleParameterTool parameter2 =
                (MultipleParameterTool)
                        createParameterToolFromArgs(
                                new String[] {
                                    "--multi",
                                    "multiValue1",
                                    "--multi",
                                    "multiValue2",
                                    "-expectedCount",
                                    "15",
                                    "--merge",
                                    "v3"
                                });
        MultipleParameterTool parameter = parameter1.mergeWith(parameter2);
        validate(parameter);
        Assert.assertEquals(Arrays.asList("v1", "v2", "v3"), parameter.getMultiParameter("merge"));
    }

    @Override
    protected AbstractParameterTool createParameterToolFromArgs(String[] args) {
        return MultipleParameterTool.fromArgs(args);
    }
}
