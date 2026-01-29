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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.catalog.FileCatalogStoreFactory;
import org.apache.flink.table.catalog.GenericInMemoryCatalogStoreFactory;
import org.apache.flink.table.secret.CommonSecretOptions;
import org.apache.flink.table.secret.GenericInMemorySecretStoreFactory;
import org.apache.flink.table.secret.SecretStoreFactory;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TableFactoryUtil}. */
class TableFactoryUtilTest {

    @ParameterizedTest(name = "kind={0}, expectedFactory={1}")
    @MethodSource("catalogStoreFactoryTestParameters")
    void testFindAndCreateCatalogStoreFactory(String kind, Class<?> expectedFactoryClass) {
        Configuration configuration = new Configuration();
        if (kind != null) {
            configuration.set(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND, kind);
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        CatalogStoreFactory factory =
                TableFactoryUtil.findAndCreateCatalogStoreFactory(configuration, classLoader);

        assertThat(factory).isInstanceOf(expectedFactoryClass);
    }

    @Test
    void testBuildCatalogStoreFactoryContext(@TempDir File tempFolder) {
        Configuration configuration = new Configuration();
        configuration.set(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND, "file");
        configuration.setString("table.catalog-store.file.path", tempFolder.getAbsolutePath());
        configuration.setString("table.catalog-store.file.option1", "value1");
        configuration.setString("table.catalog-store.file.option2", "value2");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        CatalogStoreFactory.Context context =
                TableFactoryUtil.buildCatalogStoreFactoryContext(configuration, classLoader);

        assertThat(context).isNotNull();
        assertThat(context.getOptions())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of(
                                "path", tempFolder.getAbsolutePath(),
                                "option1", "value1",
                                "option2", "value2"));
        assertThat(context.getConfiguration()).isEqualTo(configuration);
        assertThat(context.getClassLoader()).isEqualTo(classLoader);
    }

    @Test
    void testBuildCatalogStoreFactoryContextWithGenericInMemory() {
        Configuration configuration = new Configuration();
        configuration.set(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND, "generic_in_memory");
        configuration.setString("table.catalog-store.generic_in_memory.option1", "value1");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        CatalogStoreFactory.Context context =
                TableFactoryUtil.buildCatalogStoreFactoryContext(configuration, classLoader);

        assertThat(context).isNotNull();
        assertThat(context.getOptions())
                .containsExactlyInAnyOrderEntriesOf(Map.of("option1", "value1"));
        assertThat(context.getConfiguration()).isEqualTo(configuration);
        assertThat(context.getClassLoader()).isEqualTo(classLoader);
    }

    @Test
    void testBuildCatalogStoreFactoryContextWithoutOptions() {
        Configuration configuration = new Configuration();
        configuration.set(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND, "generic_in_memory");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        CatalogStoreFactory.Context context =
                TableFactoryUtil.buildCatalogStoreFactoryContext(configuration, classLoader);

        assertThat(context).isNotNull();
        assertThat(context.getOptions()).isEmpty();
        assertThat(context.getConfiguration()).isEqualTo(configuration);
        assertThat(context.getClassLoader()).isEqualTo(classLoader);
    }

    @Test
    void testBuildCatalogStoreFactoryContextOnlyExtractsRelevantOptions() {
        Configuration configuration = new Configuration();
        configuration.set(CommonCatalogOptions.TABLE_CATALOG_STORE_KIND, "file");
        configuration.setString("table.catalog-store.file.path", "/test/path");
        configuration.setString("table.catalog-store.file.option1", "value1");
        configuration.setString("table.catalog-store.other.irrelevant", "should-not-appear");
        configuration.setString("other.config.key", "should-not-appear");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        CatalogStoreFactory.Context context =
                TableFactoryUtil.buildCatalogStoreFactoryContext(configuration, classLoader);

        assertThat(context).isNotNull();
        assertThat(context.getOptions())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of("path", "/test/path", "option1", "value1"));
    }

    @ParameterizedTest(name = "kind={0}, expectedFactory={1}")
    @MethodSource("secretStoreFactoryTestParameters")
    void testFindAndCreateSecretStoreFactory(String kind, Class<?> expectedFactoryClass) {
        Configuration configuration = new Configuration();
        if (kind != null) {
            configuration.set(CommonSecretOptions.TABLE_SECRET_STORE_KIND, kind);
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        SecretStoreFactory factory =
                TableFactoryUtil.findAndCreateSecretStoreFactory(configuration, classLoader);

        assertThat(factory).isInstanceOf(expectedFactoryClass);
    }

    @Test
    void testBuildSecretStoreFactoryContext() {
        Configuration configuration = new Configuration();
        configuration.set(CommonSecretOptions.TABLE_SECRET_STORE_KIND, "generic_in_memory");
        configuration.setString("table.secret-store.generic_in_memory.option1", "value1");
        configuration.setString("table.secret-store.generic_in_memory.option2", "value2");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        SecretStoreFactory.Context context =
                TableFactoryUtil.buildSecretStoreFactoryContext(configuration, classLoader);

        assertThat(context).isNotNull();
        assertThat(context.getOptions())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of("option1", "value1", "option2", "value2"));
        assertThat(context.getConfiguration()).isEqualTo(configuration);
        assertThat(context.getClassLoader()).isEqualTo(classLoader);
    }

    @Test
    void testBuildSecretStoreFactoryContextWithoutOptions() {
        Configuration configuration = new Configuration();
        configuration.set(CommonSecretOptions.TABLE_SECRET_STORE_KIND, "generic_in_memory");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        SecretStoreFactory.Context context =
                TableFactoryUtil.buildSecretStoreFactoryContext(configuration, classLoader);

        assertThat(context).isNotNull();
        assertThat(context.getOptions()).isEmpty();
        assertThat(context.getConfiguration()).isEqualTo(configuration);
        assertThat(context.getClassLoader()).isEqualTo(classLoader);
    }

    @Test
    void testBuildSecretStoreFactoryContextOnlyExtractsRelevantOptions() {
        Configuration configuration = new Configuration();
        configuration.set(CommonSecretOptions.TABLE_SECRET_STORE_KIND, "generic_in_memory");
        configuration.setString("table.secret-store.generic_in_memory.option1", "value1");
        configuration.setString("table.secret-store.generic_in_memory.option2", "value2");
        configuration.setString("table.secret-store.other.irrelevant", "should-not-appear");
        configuration.setString("other.config.key", "should-not-appear");
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        SecretStoreFactory.Context context =
                TableFactoryUtil.buildSecretStoreFactoryContext(configuration, classLoader);

        assertThat(context).isNotNull();
        assertThat(context.getOptions())
                .containsExactlyInAnyOrderEntriesOf(
                        Map.of("option1", "value1", "option2", "value2"));
    }

    private static Stream<Arguments> catalogStoreFactoryTestParameters() {
        return Stream.of(
                Arguments.of("generic_in_memory", GenericInMemoryCatalogStoreFactory.class),
                Arguments.of("file", FileCatalogStoreFactory.class),
                Arguments.of(null, GenericInMemoryCatalogStoreFactory.class));
    }

    private static Stream<Arguments> secretStoreFactoryTestParameters() {
        return Stream.of(
                Arguments.of("generic_in_memory", GenericInMemorySecretStoreFactory.class),
                Arguments.of(null, GenericInMemorySecretStoreFactory.class));
    }
}
