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

package org.apache.flink.runtime.asyncprocessing.declare;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/** Basic UT tests for {@link DeclarationManager} and {@link DeclarationContext}. */
public class DeclarationTest {

    protected AtomicBoolean notSafe = new AtomicBoolean(false);
    @DeclarationSafe protected AtomicBoolean safe = new AtomicBoolean(false);

    @Test
    public void testSafeVariableDeclaration() throws DeclarationException {
        DeclarationManager manager = new DeclarationManager();
        DeclarationContext context = new DeclarationContext(manager);
        NamedConsumer<Integer> func1 =
                context.declare(
                        (e) -> {
                            // will not affect
                            int i = 0;
                        });
        assertThat(func1.isSafeVariables()).isTrue();

        boolean captured = false;
        NamedConsumer<Integer> func2 =
                context.declare(
                        (e) -> {
                            // final primitive variables captured will not affect
                            int a = captured ? 1 : 0;
                        });
        assertThat(func2.isSafeVariables()).isTrue();

        AtomicBoolean atomicCaptured = new AtomicBoolean(false);
        NamedConsumer<Integer> func3 =
                context.declare(
                        (e) -> {
                            // final variables captured will affect
                            int a = atomicCaptured.get() ? 1 : 0;
                        });
        assertThat(func3.isSafeVariables()).isFalse();

        NamedConsumer<Integer> func4 =
                context.declare(
                        (e) -> {
                            // not safe variables will affect
                            int a = notSafe.get() ? 1 : 0;
                        });
        assertThat(func4.isSafeVariables()).isFalse();

        NamedConsumer<Integer> func5 =
                context.declare(
                        (e) -> {
                            // safe variables will not affect
                            int a = safe.get() ? 1 : 0;
                        });
        assertThat(func5.isSafeVariables()).isFalse();
    }
}
