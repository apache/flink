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

package org.apache.flink.table.module;

import org.apache.flink.table.functions.BuiltInFunctionDefinition;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

/** Test for {@link CoreModule}. */
public class CoreModuleTest {

    @Test
    public void testListFunctions() {
        assertThat(CoreModule.INSTANCE.listFunctions(false))
                .contains("IFNULL")
                .doesNotContain("$REPLICATE_ROWS$1");

        assertThat(CoreModule.INSTANCE.listFunctions(true))
                .contains("IFNULL")
                .contains("$REPLICATE_ROWS$1");
    }

    @Test
    public void testGetNonExistFunction() {
        assertThat(CoreModule.INSTANCE.getFunctionDefinition("nonexist")).isEmpty();
    }

    @Test
    public void testGetFunction() {
        assertThat(CoreModule.INSTANCE.getFunctionDefinition("CAST"))
                .hasValueSatisfying(
                        def ->
                                assertThat(def)
                                        .asInstanceOf(type(BuiltInFunctionDefinition.class))
                                        .extracting(BuiltInFunctionDefinition::getQualifiedName)
                                        .isEqualTo("$CAST$1"));
    }

    @Test
    public void testGetInternalFunction() {
        assertThat(CoreModule.INSTANCE.getFunctionDefinition("$REPLICATE_ROWS$1"))
                .hasValueSatisfying(
                        def ->
                                assertThat(def)
                                        .asInstanceOf(type(BuiltInFunctionDefinition.class))
                                        .satisfies(
                                                builtInDef ->
                                                        assertThat(builtInDef.isInternal())
                                                                .isTrue())
                                        .satisfies(
                                                builtInDef ->
                                                        assertThat(builtInDef.getQualifiedName())
                                                                .isEqualTo("$REPLICATE_ROWS$1")));
    }
}
