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
import org.apache.flink.table.api.Schema;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

/** Test base for {@link MySqlCatalog}. */
public class MySqlCatalogTestBase {

    public static final Logger LOG = LoggerFactory.getLogger(MySqlCatalogTestBase.class);

    protected static final DockerImageName MYSQL_57_IMAGE = DockerImageName.parse("mysql:5.7.34");
    protected static final String TEST_CATALOG_NAME = "mysql_catalog";
    protected static final String TEST_USERNAME = "mysql";
    protected static final String TEST_PWD = "mysql";
    protected static final String TEST_DB = "test";
    protected static final String TEST_TABLE_ALL_TYPES = "t_all_types";
    protected static final String TEST_SINK_TABLE_ALL_TYPES = "t_all_types_sink";
    protected static final String TEST_TABLE_SINK_FROM_GROUPED_BY = "t_grouped_by_sink";
    protected static final String MYSQL_INIT_SCRIPT = "mysql-scripts/catalog-init-for-test.sql";
    protected static final Map<String, String> DEFAULT_CONTAINER_ENV_MAP =
            new HashMap<String, String>() {
                {
                    put("MYSQL_ROOT_HOST", "%");
                }
            };

    protected static final Schema TABLE_SCHEMA =
            Schema.newBuilder()
                    .column("pid", DataTypes.BIGINT().notNull())
                    .column("col_bigint", DataTypes.BIGINT())
                    .column("col_bigint_unsigned", DataTypes.DECIMAL(20, 0))
                    .column("col_binary", DataTypes.BYTES())
                    .column("col_bit", DataTypes.BOOLEAN())
                    .column("col_blob", DataTypes.BYTES())
                    .column("col_char", DataTypes.CHAR(10))
                    .column("col_date", DataTypes.DATE())
                    .column("col_datetime", DataTypes.TIMESTAMP(0))
                    .column("col_decimal", DataTypes.DECIMAL(10, 0))
                    .column("col_decimal_unsigned", DataTypes.DECIMAL(11, 0))
                    .column("col_double", DataTypes.DOUBLE())
                    .column("col_double_unsigned", DataTypes.DOUBLE())
                    .column("col_enum", DataTypes.CHAR(6))
                    .column("col_float", DataTypes.FLOAT())
                    .column("col_float_unsigned", DataTypes.FLOAT())
                    .column("col_int", DataTypes.INT())
                    .column("col_int_unsigned", DataTypes.BIGINT())
                    .column("col_integer", DataTypes.INT())
                    .column("col_integer_unsigned", DataTypes.BIGINT())
                    .column("col_json", DataTypes.STRING())
                    .column("col_longblob", DataTypes.BYTES())
                    .column("col_longtext", DataTypes.STRING())
                    .column("col_mediumblob", DataTypes.BYTES())
                    .column("col_mediumint", DataTypes.INT())
                    .column("col_mediumint_unsigned", DataTypes.INT())
                    .column("col_mediumtext", DataTypes.VARCHAR(5592405))
                    .column("col_numeric", DataTypes.DECIMAL(10, 0))
                    .column("col_numeric_unsigned", DataTypes.DECIMAL(11, 0))
                    .column("col_real", DataTypes.DOUBLE())
                    .column("col_real_unsigned", DataTypes.DOUBLE())
                    .column("col_set", DataTypes.CHAR(18))
                    .column("col_smallint", DataTypes.SMALLINT())
                    .column("col_smallint_unsigned", DataTypes.INT())
                    .column("col_text", DataTypes.VARCHAR(21845))
                    .column("col_time", DataTypes.TIME(0))
                    .column("col_timestamp", DataTypes.TIMESTAMP(0))
                    .column("col_tinytext", DataTypes.VARCHAR(85))
                    .column("col_tinyint", DataTypes.TINYINT())
                    .column("col_tinyint_unsinged", DataTypes.SMALLINT())
                    .column("col_tinyblob", DataTypes.BYTES())
                    .column("col_varchar", DataTypes.VARCHAR(255))
                    .column("col_datetime_p3", DataTypes.TIMESTAMP(3).notNull())
                    .column("col_time_p3", DataTypes.TIME(3))
                    .column("col_timestamp_p3", DataTypes.TIMESTAMP(3))
                    .column("col_varbinary", DataTypes.BYTES())
                    .primaryKeyNamed("PRIMARY", Lists.newArrayList("pid"))
                    .build();

    @ClassRule
    public static final MySQLContainer<?> MYSQL_CONTAINER =
            new MySQLContainer<>(MYSQL_57_IMAGE)
                    .withUsername("root")
                    .withPassword("")
                    .withEnv(DEFAULT_CONTAINER_ENV_MAP)
                    .withInitScript(MYSQL_INIT_SCRIPT)
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    protected static MySqlCatalog catalog;

    @BeforeClass
    public static void beforeAll() {
        String baseUrl =
                MYSQL_CONTAINER
                        .getJdbcUrl()
                        .substring(0, MYSQL_CONTAINER.getJdbcUrl().lastIndexOf("/"));
        catalog = new MySqlCatalog(TEST_CATALOG_NAME, TEST_DB, TEST_USERNAME, TEST_PWD, baseUrl);
    }
}
