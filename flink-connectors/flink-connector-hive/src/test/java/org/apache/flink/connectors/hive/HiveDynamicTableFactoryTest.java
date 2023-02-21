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

package org.apache.flink.connectors.hive;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.hive.HiveOptions.PartitionOrder;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;

import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_CLASS;
import static org.apache.flink.connector.file.table.FileSystemConnectorOptions.PARTITION_TIME_EXTRACTOR_KIND;
import static org.apache.flink.connectors.hive.HiveOptions.LOOKUP_JOIN_CACHE_TTL;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_CONSUME_START_OFFSET;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_ENABLE;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_MONITOR_INTERVAL;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_PARTITION_INCLUDE;
import static org.apache.flink.connectors.hive.HiveOptions.STREAMING_SOURCE_PARTITION_ORDER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link HiveDynamicTableFactory}. */
public class HiveDynamicTableFactoryTest {

    private static TableEnvironment tableEnv;
    private static HiveCatalog hiveCatalog;

    @BeforeClass
    public static void setup() {
        tableEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
    }

    @Test
    public void testHiveStreamingSourceOptions() throws Exception {
        // test default hive streaming-source is not a lookup source
        tableEnv.executeSql(
                String.format(
                        "create table table1 (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"
                                + " tblproperties ('%s' = 'true')",
                        STREAMING_SOURCE_ENABLE.key()));
        DynamicTableSource tableSource1 = getTableSource("table1");
        assertThat(tableSource1).isNotInstanceOf(HiveLookupTableSource.class);
        HiveTableSource tableSource = (HiveTableSource) tableSource1;
        Configuration configuration = new Configuration();
        tableSource.catalogTable.getOptions().forEach(configuration::setString);
        assertThat(configuration.get(STREAMING_SOURCE_PARTITION_ORDER))
                .isEqualTo(HiveOptions.PartitionOrder.PARTITION_NAME);

        // test table can't be selected when set 'streaming-source.partition.include' to 'latest'
        tableEnv.executeSql(
                String.format(
                        "create table table2 (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"
                                + " tblproperties ('%s' = 'true', '%s' = 'latest')",
                        STREAMING_SOURCE_ENABLE.key(), STREAMING_SOURCE_PARTITION_INCLUDE.key()));
        DynamicTableSource tableSource2 = getTableSource("table2");
        assertThat(tableSource2).isInstanceOf(HiveLookupTableSource.class);
        assertThatThrownBy(() -> tableEnv.executeSql("select * from table2"))
                .hasMessage(
                        "The only supported 'streaming-source.partition.include' is 'all' in"
                                + " hive table scan, but is 'latest'");

        // test table support 'partition-name' in option 'streaming-source.partition.order'.
        tableEnv.executeSql(
                String.format(
                        "create table table3 (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"
                                + " tblproperties ('%s' = 'true', '%s' = 'partition-name')",
                        STREAMING_SOURCE_ENABLE.key(), STREAMING_SOURCE_PARTITION_ORDER.key()));
        DynamicTableSource tableSource3 = getTableSource("table3");
        assertThat(tableSource3).isInstanceOf(HiveTableSource.class);
        HiveTableSource hiveTableSource3 = (HiveTableSource) tableSource3;
        Configuration configuration1 = new Configuration();
        hiveTableSource3.catalogTable.getOptions().forEach(configuration1::setString);
        PartitionOrder partitionOrder1 = configuration1.get(STREAMING_SOURCE_PARTITION_ORDER);
        assertThat(partitionOrder1).isEqualTo(HiveOptions.PartitionOrder.PARTITION_NAME);

        // test deprecated option key 'streaming-source.consume-order' and new key
        // 'streaming-source.partition-order'
        tableEnv.executeSql(
                String.format(
                        "create table table4 (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"
                                + " tblproperties ('%s' = 'true', '%s' = 'partition-time')",
                        STREAMING_SOURCE_ENABLE.key(), "streaming-source.consume-order"));
        DynamicTableSource tableSource4 = getTableSource("table4");
        assertThat(tableSource4).isInstanceOf(HiveTableSource.class);
        HiveTableSource hiveTableSource = (HiveTableSource) tableSource4;

        Configuration configuration2 = new Configuration();
        hiveTableSource.catalogTable.getOptions().forEach(configuration2::setString);
        PartitionOrder partitionOrder2 = configuration2.get(STREAMING_SOURCE_PARTITION_ORDER);
        assertThat(partitionOrder2).isEqualTo(HiveOptions.PartitionOrder.PARTITION_TIME);
    }

    @Test
    public void testHiveLookupSourceOptions() throws Exception {
        // test hive bounded source is a lookup source
        tableEnv.executeSql(
                String.format(
                        "create table table5 (x int, y string, z int) tblproperties ('%s'='5min')",
                        LOOKUP_JOIN_CACHE_TTL.key()));
        DynamicTableSource tableSource1 = getTableSource("table5");
        assertThat(tableSource1).isInstanceOf(HiveLookupTableSource.class);

        // test hive streaming source is a lookup source when 'streaming-source.partition.include' =
        // 'latest'
        tableEnv.executeSql(
                String.format(
                        "create table table6 (x int, y string, z int)"
                                + " tblproperties ('%s' = 'true', '%s' = 'latest')",
                        STREAMING_SOURCE_ENABLE.key(), STREAMING_SOURCE_PARTITION_INCLUDE.key()));
        DynamicTableSource tableSource2 = getTableSource("table6");
        assertThat(tableSource2).isInstanceOf(HiveLookupTableSource.class);
        FileSystemLookupFunction lookupFunction =
                (FileSystemLookupFunction)
                        ((HiveLookupTableSource) tableSource2).getLookupFunction(new int[][] {{0}});
        // test default lookup cache ttl for streaming-source is 1 hour
        assertThat(lookupFunction.getReloadInterval()).isEqualTo(Duration.ofHours(1));
        HiveLookupTableSource lookupTableSource = (HiveLookupTableSource) tableSource2;
        Configuration configuration = new Configuration();
        lookupTableSource.catalogTable.getOptions().forEach(configuration::setString);
        assertThat(HiveOptions.PartitionOrder.PARTITION_NAME)
                .isEqualTo(configuration.get(STREAMING_SOURCE_PARTITION_ORDER));

        // test lookup with partition-time extractor options
        tableEnv.executeSql(
                String.format(
                        "create table table7 (x int, y string, z int)"
                                + " tblproperties ("
                                + "'%s' = 'true',"
                                + " '%s' = 'latest',"
                                + " '%s' = '120min',"
                                + " '%s' = 'partition-time', "
                                + " '%s' = 'custom',"
                                + " '%s' = 'path.to..TimeExtractor')",
                        STREAMING_SOURCE_ENABLE.key(),
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        STREAMING_SOURCE_MONITOR_INTERVAL.key(),
                        STREAMING_SOURCE_PARTITION_ORDER.key(),
                        PARTITION_TIME_EXTRACTOR_KIND.key(),
                        PARTITION_TIME_EXTRACTOR_CLASS.key()));

        DynamicTableSource tableSource3 = getTableSource("table7");
        assertThat(tableSource3).isInstanceOf(HiveLookupTableSource.class);
        HiveLookupTableSource tableSource = (HiveLookupTableSource) tableSource3;
        Configuration configuration1 = new Configuration();
        tableSource.catalogTable.getOptions().forEach(configuration1::setString);

        assertThat(HiveOptions.PartitionOrder.PARTITION_TIME)
                .isEqualTo(configuration1.get(STREAMING_SOURCE_PARTITION_ORDER));
        assertThat("custom").isEqualTo(configuration1.get(PARTITION_TIME_EXTRACTOR_KIND));
        assertThat("path.to..TimeExtractor")
                .isEqualTo(configuration1.get(PARTITION_TIME_EXTRACTOR_CLASS));

        tableEnv.executeSql(
                String.format(
                        "create table table8 (x int, y string, z int)"
                                + " tblproperties ('%s' = 'true', '%s' = 'latest', '%s' = '5min')",
                        STREAMING_SOURCE_ENABLE.key(),
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        STREAMING_SOURCE_MONITOR_INTERVAL.key()));
        DynamicTableSource tableSource4 = getTableSource("table8");
        assertThat(tableSource4).isInstanceOf(HiveLookupTableSource.class);
        HiveLookupTableSource lookupTableSource4 = (HiveLookupTableSource) tableSource4;
        Configuration configuration4 = new Configuration();
        lookupTableSource4.catalogTable.getOptions().forEach(configuration4::setString);
        assertThat(Duration.ofMinutes(5L))
                .isEqualTo(configuration4.get(STREAMING_SOURCE_MONITOR_INTERVAL));
    }

    @Test
    public void testInvalidOptions() {
        tableEnv.executeSql(
                String.format(
                        "create table table9 (x int, y string, z int)"
                                + " tblproperties ('%s' = 'true', '%s' = 'latest', '%s' = '120min', '%s' = '1970-00-01')",
                        STREAMING_SOURCE_ENABLE.key(),
                        STREAMING_SOURCE_PARTITION_INCLUDE.key(),
                        STREAMING_SOURCE_MONITOR_INTERVAL.key(),
                        STREAMING_SOURCE_CONSUME_START_OFFSET.key()));
        assertThatThrownBy(() -> getTableSource("table9"))
                .rootCause()
                .hasMessage(
                        "The 'streaming-source.consume-start-offset' is not supported when "
                                + "set 'streaming-source.partition.include' to 'latest'");
    }

    @Test
    public void testJobConfWithCredentials() throws Exception {
        final Text hdfsDelegationTokenKind = new Text("HDFS_DELEGATION_TOKEN");
        final Text hdfsDelegationTokenService = new Text("ha-hdfs:hadoop-namespace");
        Credentials credentials = new Credentials();
        credentials.addToken(
                hdfsDelegationTokenService,
                new Token<>(
                        new byte[4],
                        new byte[4],
                        hdfsDelegationTokenKind,
                        hdfsDelegationTokenService));
        UserGroupInformation.getCurrentUser().addCredentials(credentials);

        // test table source's jobConf with credentials
        tableEnv.executeSql(
                String.format(
                        "create table table10 (x int, y string, z int) partitioned by ("
                                + " pt_year int, pt_mon string, pt_day string)"));
        DynamicTableSource tableSource1 = getTableSource("table10");

        HiveTableSource tableSource = (HiveTableSource) tableSource1;
        Token token =
                tableSource.getJobConf().getCredentials().getToken(hdfsDelegationTokenService);
        assertThat(token).isNotNull();
        assertThat(token.getKind()).isEqualTo(hdfsDelegationTokenKind);
        assertThat(token.getService()).isEqualTo(hdfsDelegationTokenService);

        // test table sink's jobConf with credentials
        DynamicTableSink tableSink1 = getTableSink("table10");
        HiveTableSink tableSink = (HiveTableSink) tableSink1;
        token = tableSink.getJobConf().getCredentials().getToken(hdfsDelegationTokenService);
        assertThat(token).isNotNull();
        assertThat(token.getKind()).isEqualTo(hdfsDelegationTokenKind);
        assertThat(token.getService()).isEqualTo(hdfsDelegationTokenService);
    }

    private DynamicTableSource getTableSource(String tableName) throws Exception {
        TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of(hiveCatalog.getName(), "default", tableName);
        CatalogTable catalogTable =
                (CatalogTable) hiveCatalog.getTable(tableIdentifier.toObjectPath());
        return FactoryUtil.createDynamicTableSource(
                (DynamicTableSourceFactory)
                        hiveCatalog.getFactory().orElseThrow(IllegalStateException::new),
                tableIdentifier,
                tableEnvInternal.getCatalogManager().resolveCatalogTable(catalogTable),
                tableEnv.getConfig(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }

    private DynamicTableSink getTableSink(String tableName) throws Exception {
        TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
        ObjectIdentifier tableIdentifier =
                ObjectIdentifier.of(hiveCatalog.getName(), "default", tableName);
        CatalogTable catalogTable =
                (CatalogTable) hiveCatalog.getTable(tableIdentifier.toObjectPath());
        return FactoryUtil.createDynamicTableSink(
                (DynamicTableSinkFactory)
                        hiveCatalog.getFactory().orElseThrow(IllegalStateException::new),
                tableIdentifier,
                tableEnvInternal.getCatalogManager().resolveCatalogTable(catalogTable),
                tableEnv.getConfig(),
                Thread.currentThread().getContextClassLoader(),
                false);
    }
}
