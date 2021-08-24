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

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

/** Test base for {@link MySQLCatalog}. */
public class MySQLCatalogTestBase {

    public static final Logger LOG = LoggerFactory.getLogger(MySQLCatalogTestBase.class);

    protected static final DockerImageName MYSQL_57_IMAGE = DockerImageName.parse("mysql:5.7.34");
    protected static final String TEST_CATALOG_NAME = "mysql_catalog";
    protected static final String TEST_USERNAME = "mysql";
    protected static final String TEST_PWD = "mysql";
    protected static final String TEST_DB = "test";
    protected static final String TEST_TABLE_ALL_TYPES = "t_all_types";
    protected static final String TEST_SINK_TABLE_ALL_TYPES_WITHOUT_YEAR_TYPE =
            "t_all_types_sink_without_year_type";
    protected static final String TEST_SINK_TABLE_ALL_TYPES_WITH_YEAR_TYPE =
            "t_all_types_sink_with_year_type";
    protected static final String TEST_TABLE_SINK_FROM_GROUPED_BY = "t_grouped_by_sink";
    protected static final String MYSQL_INIT_SCRIPT = "mysql-scripts/catalog-init-for-test.sql";
    protected static final Map<String, String> DEFAULT_CONTAINER_ENV_MAP =
            new HashMap<String, String>() {
                {
                    put("MYSQL_ROOT_HOST", "%");
                }
            };

    protected static final TableSchema TABLESCHEMA =
            TableSchema.builder()
                    .field("pid", DataTypes.BIGINT().notNull())
                    .field("col_bigint", DataTypes.BIGINT())
                    .field("col_bigint_unsigned", DataTypes.DECIMAL(20, 0))
                    .field("col_binary", DataTypes.BYTES())
                    .field("col_bit", DataTypes.BOOLEAN())
                    .field("col_blob", DataTypes.BYTES())
                    .field("col_char", DataTypes.STRING())
                    .field("col_date", DataTypes.DATE())
                    .field("col_datetime", DataTypes.TIMESTAMP(0))
                    .field("col_decimal", DataTypes.DECIMAL(10, 0))
                    .field("col_decimal_unsigned", DataTypes.DECIMAL(10, 0))
                    .field("col_double", DataTypes.DOUBLE())
                    .field("col_double_unsigned", DataTypes.DOUBLE())
                    .field("col_enum", DataTypes.STRING())
                    .field("col_float", DataTypes.FLOAT())
                    .field("col_float_unsigned", DataTypes.FLOAT())
                    .field("col_geometry", DataTypes.BYTES())
                    .field("col_geometrycollection", DataTypes.BYTES())
                    .field("col_int", DataTypes.INT())
                    .field("col_int_unsigned", DataTypes.BIGINT())
                    .field("col_integer", DataTypes.INT())
                    .field("col_integer_unsigned", DataTypes.BIGINT())
                    .field("col_json", DataTypes.STRING())
                    .field("col_linestring", DataTypes.BYTES())
                    .field("col_longblob", DataTypes.BYTES())
                    .field("col_longtext", DataTypes.STRING())
                    .field("col_mediumblob", DataTypes.BYTES())
                    .field("col_mediumint", DataTypes.INT())
                    .field("col_mediumint_unsigned", DataTypes.INT())
                    .field("col_mediumtext", DataTypes.STRING())
                    .field("col_multilinestring", DataTypes.BYTES())
                    .field("col_multipoint", DataTypes.BYTES())
                    .field("col_multipolygon", DataTypes.BYTES())
                    .field("col_numeric", DataTypes.DECIMAL(10, 0))
                    .field("col_numeric_unsigned", DataTypes.DECIMAL(10, 0))
                    .field("col_polygon", DataTypes.BYTES())
                    .field("col_point", DataTypes.BYTES())
                    .field("col_real", DataTypes.DOUBLE())
                    .field("col_real_unsigned", DataTypes.DOUBLE())
                    .field("col_set", DataTypes.STRING())
                    .field("col_smallint", DataTypes.SMALLINT())
                    .field("col_smallint_unsigned", DataTypes.INT())
                    .field("col_text", DataTypes.STRING())
                    .field("col_time", DataTypes.TIME(0))
                    .field("col_timestamp", DataTypes.TIMESTAMP(0))
                    .field("col_tinytext", DataTypes.STRING())
                    .field("col_tinyint", DataTypes.TINYINT())
                    .field("col_tinyint_unsinged", DataTypes.TINYINT())
                    .field("col_tinyblob", DataTypes.BYTES())
                    .field("col_varchar", DataTypes.STRING())
                    .field("col_year", DataTypes.DATE())
                    .field("col_datetime_p3", DataTypes.TIMESTAMP(3).notNull())
                    .field("col_time_p3", DataTypes.TIME(3))
                    .field("col_timestamp_p3", DataTypes.TIMESTAMP(3))
                    .field("col_varbinary", DataTypes.BYTES())
                    .primaryKey("PRIMARY", new String[] {"pid"})
                    .build();

    public static final MySQLContainer<?> MYSQL_CONTAINER =
            new MySQLContainer<>(MYSQL_57_IMAGE)
                    .withUsername("root")
                    .withPassword("")
                    .withEnv(DEFAULT_CONTAINER_ENV_MAP)
                    .withInitScript(MYSQL_INIT_SCRIPT)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected static String baseUrl;

    @Rule public ExpectedException exception = ExpectedException.none();
    protected static MySQLCatalog catalog;

    @BeforeClass
    public static void launchContainer() {
        MYSQL_CONTAINER.start();
        // Constructs the baseUrl.
        baseUrl =
                MYSQL_CONTAINER
                        .getJdbcUrl()
                        .substring(0, MYSQL_CONTAINER.getJdbcUrl().lastIndexOf("/"));
        catalog = new MySQLCatalog(TEST_CATALOG_NAME, TEST_DB, TEST_USERNAME, TEST_PWD, baseUrl);
    }

    @AfterClass
    public static void shutdownContainer() {
        MYSQL_CONTAINER.stop();
        MYSQL_CONTAINER.close();
    }
}
