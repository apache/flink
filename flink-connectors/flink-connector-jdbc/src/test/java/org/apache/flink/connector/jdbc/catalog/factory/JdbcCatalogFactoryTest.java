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

package org.apache.flink.connector.jdbc.catalog.factory;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.catalog.PostgresCatalog;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.DockerImageVersions;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link JdbcCatalogFactory}. */
public class JdbcCatalogFactoryTest {

    public static final Logger LOG = LoggerFactory.getLogger(JdbcCatalogFactoryTest.class);

    protected static String baseUrl;
    protected static JdbcCatalog catalog;

    protected static final String TEST_CATALOG_NAME = "mypg";
    protected static final String TEST_USERNAME = "postgres";
    protected static final String TEST_PWD = "postgres";

    protected static final DockerImageName POSTGRES_IMAGE =
            DockerImageName.parse(DockerImageVersions.POSTGRES);

    @ClassRule
    public static final PostgreSQLContainer<?> POSTGRES_CONTAINER =
            new PostgreSQLContainer<>(POSTGRES_IMAGE)
                    .withUsername(TEST_USERNAME)
                    .withPassword(TEST_PWD)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeClass
    public static void setup() throws SQLException {
        // jdbc:postgresql://localhost:50807/postgres?user=postgres
        String jdbcUrl = POSTGRES_CONTAINER.getJdbcUrl();
        // jdbc:postgresql://localhost:50807/
        baseUrl = jdbcUrl.substring(0, jdbcUrl.lastIndexOf("/"));

        catalog =
                new JdbcCatalog(
                        TEST_CATALOG_NAME,
                        PostgresCatalog.DEFAULT_DATABASE,
                        TEST_USERNAME,
                        TEST_PWD,
                        baseUrl);
    }

    @Test
    public void test() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), JdbcCatalogFactoryOptions.IDENTIFIER);
        options.put(
                JdbcCatalogFactoryOptions.DEFAULT_DATABASE.key(), PostgresCatalog.DEFAULT_DATABASE);
        options.put(JdbcCatalogFactoryOptions.USERNAME.key(), TEST_USERNAME);
        options.put(JdbcCatalogFactoryOptions.PASSWORD.key(), TEST_PWD);
        options.put(JdbcCatalogFactoryOptions.BASE_URL.key(), baseUrl);

        final Catalog actualCatalog =
                FactoryUtil.createCatalog(
                        TEST_CATALOG_NAME,
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());

        checkEquals(catalog, (JdbcCatalog) actualCatalog);

        assertThat(((JdbcCatalog) actualCatalog).getInternal()).isInstanceOf(PostgresCatalog.class);
    }

    private static void checkEquals(JdbcCatalog c1, JdbcCatalog c2) {
        assertThat(c2.getName()).isEqualTo(c1.getName());
        assertThat(c2.getDefaultDatabase()).isEqualTo(c1.getDefaultDatabase());
        assertThat(c2.getUsername()).isEqualTo(c1.getUsername());
        assertThat(c2.getPassword()).isEqualTo(c1.getPassword());
        assertThat(c2.getBaseUrl()).isEqualTo(c1.getBaseUrl());
    }
}
