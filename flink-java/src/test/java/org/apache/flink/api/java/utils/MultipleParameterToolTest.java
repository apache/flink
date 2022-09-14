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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.data.Offset.offset;

/** Tests for {@link MultipleParameterTool}. */
class MultipleParameterToolTest extends AbstractParameterToolTest {

    @Test
    void testFromCliArgsWithMultipleParameters() {
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

        assertThat(parameter.getNumberOfParameters()).isEqualTo(8);
        validate(parameter);
        assertThat(parameter.has("withoutValues")).isTrue();
        assertThat(parameter.getFloat("negativeFloat")).isCloseTo(-0.58f, offset(0.1f));
        assertThat(parameter.getBoolean("isWorking")).isTrue();
        assertThat(parameter.getByte("maxByte")).isEqualTo((byte) 127);
        assertThat(parameter.getShort("negativeShort")).isEqualTo((short) -1024);

        assertThatThrownBy(() -> parameter.get("multi"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Key multi should has only one value");
    }

    @Test
    void testUnrequestedMultiParameter() {
        MultipleParameterTool parameter =
                (MultipleParameterTool)
                        createParameterToolFromArgs(
                                new String[] {"--multi", "v1", "--multi", "v2", "--multi2", "vv1"});
        assertThat(parameter.getUnrequestedParameters())
                .containsExactlyInAnyOrder("multi", "multi2");

        assertThat(parameter.getMultiParameter("multi")).containsExactly("v1", "v2");
        assertThat(parameter.getUnrequestedParameters()).containsExactlyInAnyOrder("multi2");

        assertThat(parameter.getMultiParameterRequired("multi2")).containsExactly("vv1");
        assertThat(parameter.getUnrequestedParameters()).isEmpty();
    }

    @Test
    void testMerged() {
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
        assertThat(parameter.getMultiParameter("merge")).containsExactly("v1", "v2", "v3");
    }

    @Override
    protected AbstractParameterTool createParameterToolFromArgs(String[] args) {
        return MultipleParameterTool.fromArgs(args);
    }
}
