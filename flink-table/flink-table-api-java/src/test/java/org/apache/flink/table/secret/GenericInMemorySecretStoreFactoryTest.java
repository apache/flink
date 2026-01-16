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

package org.apache.flink.table.secret;

import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.FactoryUtil.DefaultSecretStoreContext;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Test for {@link GenericInMemorySecretStoreFactory}. */
public class GenericInMemorySecretStoreFactoryTest {

    @Test
    void testSecretStoreInit() {
        String factoryIdentifier = GenericInMemorySecretStoreFactory.IDENTIFIER;
        Map<String, String> options = new HashMap<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        final DefaultSecretStoreContext discoveryContext =
                new DefaultSecretStoreContext(options, null, classLoader);
        final SecretStoreFactory factory =
                FactoryUtil.discoverFactory(
                        classLoader, SecretStoreFactory.class, factoryIdentifier);
        factory.open(discoveryContext);

        SecretStore secretStore = factory.createSecretStore();
        assertThat(secretStore instanceof GenericInMemorySecretStore).isTrue();

        factory.close();
    }

    @Test
    void testCreateSecretStoreBeforeOpen() {
        GenericInMemorySecretStoreFactory factory = new GenericInMemorySecretStoreFactory();

        assertThatThrownBy(factory::createSecretStore)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(
                        "SecretStoreFactory must be opened before creating a SecretStore");
    }

    @Test
    void testFactoryIdentifier() {
        GenericInMemorySecretStoreFactory factory = new GenericInMemorySecretStoreFactory();
        assertThat(factory.factoryIdentifier()).isEqualTo("generic_in_memory");
    }

    @Test
    void testRequiredAndOptionalOptions() {
        GenericInMemorySecretStoreFactory factory = new GenericInMemorySecretStoreFactory();
        assertThat(factory.requiredOptions().isEmpty()).isTrue();
        assertThat(factory.optionalOptions().isEmpty()).isTrue();
    }

    @Test
    void testOpenAndClose() {
        GenericInMemorySecretStoreFactory factory = new GenericInMemorySecretStoreFactory();
        Map<String, String> options = new HashMap<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        DefaultSecretStoreContext context =
                new DefaultSecretStoreContext(options, null, classLoader);

        factory.open(context);
        SecretStore secretStore = factory.createSecretStore();
        assertThat(secretStore).isNotNull();

        factory.close();

        // After close, createSecretStore should fail
        assertThatThrownBy(factory::createSecretStore).isInstanceOf(IllegalStateException.class);
    }
}
