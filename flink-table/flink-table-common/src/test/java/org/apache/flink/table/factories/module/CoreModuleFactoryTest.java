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

package org.apache.flink.table.factories.module;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.module.CoreModule;
import org.apache.flink.table.module.CoreModuleFactory;
import org.apache.flink.table.module.Module;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link org.apache.flink.table.module.CoreModule} created by {@link
 * org.apache.flink.table.module.CoreModuleFactory}.
 */
public class CoreModuleFactoryTest {

    @Test
    public void test() {
        final CoreModule expectedModule = CoreModule.INSTANCE;

        final Module actualModule =
                FactoryUtil.createModule(
                        CoreModuleFactory.IDENTIFIER,
                        Collections.emptyMap(),
                        new Configuration(),
                        Thread.currentThread().getContextClassLoader());

        assertEquals(expectedModule, actualModule);
    }
}
