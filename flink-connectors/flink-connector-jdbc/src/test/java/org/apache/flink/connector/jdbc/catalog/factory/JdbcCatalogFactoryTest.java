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
import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.JdbcCatalogDescriptor;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.TableFactoryService;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.SingleInstancePostgresRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test for {@link JdbcCatalogFactory}. */
public class JdbcCatalogFactoryTest {
    @ClassRule public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

    protected static String baseUrl;
    protected static JdbcCatalog catalog;

    protected static final String TEST_CATALOG_NAME = "mypg";
    protected static final String TEST_USERNAME = "postgres";
    protected static final String TEST_PWD = "postgres";

    @BeforeClass
    public static void setup() throws SQLException {
        // jdbc:postgresql://localhost:50807/postgres?user=postgres
        String embeddedJdbcUrl = pg.getEmbeddedPostgres().getJdbcUrl(TEST_USERNAME, TEST_PWD);
        // jdbc:postgresql://localhost:50807/
        baseUrl = embeddedJdbcUrl.substring(0, embeddedJdbcUrl.lastIndexOf("/") + 1);

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
        final CatalogDescriptor catalogDescriptor =
                new JdbcCatalogDescriptor(
                        PostgresCatalog.DEFAULT_DATABASE, TEST_USERNAME, TEST_PWD, baseUrl);

        final Map<String, String> properties = catalogDescriptor.toProperties();

        final Catalog actualCatalog =
                TableFactoryService.find(CatalogFactory.class, properties)
                        .createCatalog(TEST_CATALOG_NAME, properties);

        checkEquals(catalog, (JdbcCatalog) actualCatalog);

        assertTrue(((JdbcCatalog) actualCatalog).getInternal() instanceof PostgresCatalog);
    }

    private static void checkEquals(JdbcCatalog c1, JdbcCatalog c2) {
        assertEquals(c1.getName(), c2.getName());
        assertEquals(c1.getDefaultDatabase(), c2.getDefaultDatabase());
        assertEquals(c1.getUsername(), c2.getUsername());
        assertEquals(c1.getPassword(), c2.getPassword());
        assertEquals(c1.getBaseUrl(), c2.getBaseUrl());
    }
}
