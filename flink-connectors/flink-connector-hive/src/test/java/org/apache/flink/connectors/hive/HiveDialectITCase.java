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

import org.apache.flink.sql.parser.hive.ddl.SqlCreateHiveTable;
import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogView;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.planner.delegation.hive.HiveParser;
import org.apache.flink.table.utils.CatalogManagerMocks;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test Hive syntax when Hive dialect is used. */
public class HiveDialectITCase {

    private TableEnvironment tableEnv;
    private HiveCatalog hiveCatalog;
    private String warehouse;

    @Before
    public void setup() {
        hiveCatalog = HiveTestUtils.createHiveCatalog();
        hiveCatalog
                .getHiveConf()
                .setBoolVar(
                        HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES, false);
        hiveCatalog.open();
        warehouse = hiveCatalog.getHiveConf().getVar(HiveConf.ConfVars.METASTOREWAREHOUSE);
        tableEnv = HiveTestUtils.createTableEnvInBatchMode();
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.registerCatalog(hiveCatalog.getName(), hiveCatalog);
        tableEnv.useCatalog(hiveCatalog.getName());
    }

    @After
    public void tearDown() {
        if (hiveCatalog != null) {
            hiveCatalog.close();
        }
        if (warehouse != null) {
            FileUtils.deleteDirectoryQuietly(new File(warehouse));
        }
    }

    @Test
    public void testPluggableParser() {
        TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
        Parser parser = tableEnvInternal.getParser();
        // hive dialect should use HiveParser
        assertThat(parser).isInstanceOf(HiveParser.class);
        // execute some sql and verify the parser instance is reused
        tableEnvInternal.executeSql("show databases");
        assertThat(tableEnvInternal.getParser()).isSameAs(parser);
        // switching dialect will result in a new parser
        tableEnvInternal.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        assertThat(tableEnvInternal.getParser().getClass().getName())
                .isNotEqualTo(parser.getClass().getName());
    }

    @Test
    public void testParseCommand() {
        TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
        Parser parser = tableEnvInternal.getParser();

        // hive dialect should use HiveParser
        assertThat(parser).isInstanceOf(HiveParser.class);
        assertThat(parser.parse("HELP").get(0)).isInstanceOf(HelpOperation.class);
        assertThat(parser.parse("clear").get(0)).isInstanceOf(ClearOperation.class);
        assertThat(parser.parse("SET").get(0)).isInstanceOf(SetOperation.class);
        assertThat(parser.parse("ResET").get(0)).isInstanceOf(ResetOperation.class);
        assertThat(parser.parse("Exit").get(0)).isInstanceOf(QuitOperation.class);
    }

    @Test
    public void testCreateDatabase() throws Exception {
        tableEnv.executeSql("create database db1 comment 'db1 comment'");
        Database db = hiveCatalog.getHiveDatabase("db1");
        assertThat(db.getDescription()).isEqualTo("db1 comment");

        String db2Location = warehouse + "/db2_location";
        tableEnv.executeSql(
                String.format(
                        "create database db2 location '%s' with dbproperties('k1'='v1')",
                        db2Location));
        db = hiveCatalog.getHiveDatabase("db2");
        assertThat(locationPath(db.getLocationUri())).isEqualTo(db2Location);
        assertThat(db.getParameters().get("k1")).isEqualTo("v1");
    }

    @Test
    public void testAlterDatabase() throws Exception {
        // alter properties
        tableEnv.executeSql("create database db1 with dbproperties('k1'='v1')");
        tableEnv.executeSql("alter database db1 set dbproperties ('k1'='v11','k2'='v2')");
        Database db = hiveCatalog.getHiveDatabase("db1");
        assertThat(db.getParameters().get("k1")).isEqualTo("v11");
        assertThat(db.getParameters().get("k2")).isEqualTo("v2");

        // alter owner
        tableEnv.executeSql("alter database db1 set owner user user1");
        db = hiveCatalog.getHiveDatabase("db1");
        assertThat(db.getOwnerName()).isEqualTo("user1");
        assertThat(db.getOwnerType()).isEqualTo(PrincipalType.USER);

        tableEnv.executeSql("alter database db1 set owner role role1");
        db = hiveCatalog.getHiveDatabase("db1");
        assertThat(db.getOwnerName()).isEqualTo("role1");
        assertThat(db.getOwnerType()).isEqualTo(PrincipalType.ROLE);

        // alter location
        if (hiveCatalog.getHiveVersion().compareTo("2.4.0") >= 0) {
            String newLocation = warehouse + "/db1_new_location";
            tableEnv.executeSql(String.format("alter database db1 set location '%s'", newLocation));
            db = hiveCatalog.getHiveDatabase("db1");
            assertThat(locationPath(db.getLocationUri())).isEqualTo(newLocation);
        }
    }

    @Test
    public void testCreateTable() throws Exception {
        String location = warehouse + "/external_location";
        tableEnv.executeSql(
                String.format(
                        "create external table tbl1 (d decimal(10,0),ts timestamp) partitioned by (p string) location '%s' tblproperties('k1'='v1')",
                        location));
        Table hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl1"));
        assertThat(hiveTable.getTableType()).isEqualTo(TableType.EXTERNAL_TABLE.toString());
        assertThat(hiveTable.getPartitionKeysSize()).isEqualTo(1);
        assertThat(locationPath(hiveTable.getSd().getLocation())).isEqualTo(location);
        assertThat(hiveTable.getParameters().get("k1")).isEqualTo("v1");
        assertThat(hiveTable.getParameters())
                .doesNotContainKey(SqlCreateHiveTable.TABLE_LOCATION_URI);

        tableEnv.executeSql("create table tbl2 (s struct<ts:timestamp,bin:binary>) stored as orc");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl2"));
        assertThat(hiveTable.getTableType()).isEqualTo(TableType.MANAGED_TABLE.toString());
        assertThat(hiveTable.getSd().getSerdeInfo().getSerializationLib())
                .isEqualTo(OrcSerde.class.getName());
        assertThat(hiveTable.getSd().getInputFormat()).isEqualTo(OrcInputFormat.class.getName());
        assertThat(hiveTable.getSd().getOutputFormat()).isEqualTo(OrcOutputFormat.class.getName());

        tableEnv.executeSql(
                "create table tbl3 (m map<timestamp,binary>) partitioned by (p1 bigint,p2 tinyint) "
                        + "row format serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe'");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl3"));
        assertThat(hiveTable.getPartitionKeysSize()).isEqualTo(2);
        assertThat(hiveTable.getSd().getSerdeInfo().getSerializationLib())
                .isEqualTo(LazyBinarySerDe.class.getName());

        tableEnv.executeSql(
                "create table tbl4 (x int,y smallint) row format delimited fields terminated by '|' lines terminated by '\n'");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl4"));
        assertThat(hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.FIELD_DELIM))
                .isEqualTo("|");
        assertThat(
                        hiveTable
                                .getSd()
                                .getSerdeInfo()
                                .getParameters()
                                .get(serdeConstants.SERIALIZATION_FORMAT))
                .isEqualTo("|");
        assertThat(hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.LINE_DELIM))
                .isEqualTo("\n");

        tableEnv.executeSql(
                "create table tbl5 (m map<bigint,string>) row format delimited collection items terminated by ';' "
                        + "map keys terminated by ':'");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl5"));
        assertThat(
                        hiveTable
                                .getSd()
                                .getSerdeInfo()
                                .getParameters()
                                .get(serdeConstants.COLLECTION_DELIM))
                .isEqualTo(";");
        assertThat(
                        hiveTable
                                .getSd()
                                .getSerdeInfo()
                                .getParameters()
                                .get(serdeConstants.MAPKEY_DELIM))
                .isEqualTo(":");

        int createdTimeForTableExists = hiveTable.getCreateTime();
        tableEnv.executeSql("create table if not exists tbl5 (m map<bigint,string>)");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl5"));
        assertThat(hiveTable.getCreateTime()).isEqualTo(createdTimeForTableExists);

        // test describe table
        Parser parser = ((TableEnvironmentInternal) tableEnv).getParser();
        DescribeTableOperation operation =
                (DescribeTableOperation) parser.parse("desc tbl1").get(0);
        assertThat(operation.isExtended()).isFalse();
        assertThat(operation.getSqlIdentifier())
                .isEqualTo(ObjectIdentifier.of(hiveCatalog.getName(), "default", "tbl1"));

        operation = (DescribeTableOperation) parser.parse("describe default.tbl2").get(0);
        assertThat(operation.isExtended()).isFalse();
        assertThat(operation.getSqlIdentifier())
                .isEqualTo(ObjectIdentifier.of(hiveCatalog.getName(), "default", "tbl2"));

        operation = (DescribeTableOperation) parser.parse("describe extended tbl3").get(0);
        assertThat(operation.isExtended()).isTrue();
        assertThat(operation.getSqlIdentifier())
                .isEqualTo(ObjectIdentifier.of(hiveCatalog.getName(), "default", "tbl3"));
    }

    @Test
    public void testCreateTableWithConstraints() throws Exception {
        Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
        tableEnv.executeSql(
                "create table tbl (x int,y int not null disable novalidate rely,z int not null disable novalidate norely,"
                        + "constraint pk_name primary key (x) disable rely)");
        CatalogTable catalogTable =
                (CatalogTable) hiveCatalog.getTable(new ObjectPath("default", "tbl"));
        TableSchema tableSchema = catalogTable.getSchema();
        assertThat(tableSchema.getPrimaryKey()).as("PK not present").isPresent();
        assertThat(tableSchema.getPrimaryKey().get().getName()).isEqualTo("pk_name");
        assertThat(tableSchema.getFieldDataTypes()[0].getLogicalType().isNullable())
                .as("PK cannot be null")
                .isFalse();
        assertThat(tableSchema.getFieldDataTypes()[1].getLogicalType().isNullable())
                .as("RELY NOT NULL should be reflected in schema")
                .isFalse();
        assertThat(tableSchema.getFieldDataTypes()[2].getLogicalType().isNullable())
                .as("NORELY NOT NULL shouldn't be reflected in schema")
                .isTrue();
    }

    @Test
    public void testCreateTableAs() throws Exception {
        tableEnv.executeSql("create table src (x int,y string)");
        tableEnv.executeSql("create table tbl1 as select x from src group by x").await();
        Table hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl1"));
        assertThat(hiveTable.getSd().getCols()).hasSize(1);
        assertThat(hiveTable.getSd().getCols().get(0).getName()).isEqualTo("x");
        assertThat(hiveTable.getSd().getCols().get(0).getType()).isEqualTo("int");
        tableEnv.executeSql(
                        "create table default.tbl2 stored as orc as select x,max(y) as m from src group by x order by x limit 1")
                .await();
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl2"));
        assertThat(hiveTable.getSd().getCols()).hasSize(2);
        assertThat(hiveTable.getSd().getCols().get(0).getName()).isEqualTo("x");
        assertThat(hiveTable.getSd().getCols().get(1).getName()).isEqualTo("m");
        assertThat(hiveTable.getSd().getSerdeInfo().getSerializationLib())
                .isEqualTo(OrcSerde.class.getName());
        assertThat(hiveTable.getSd().getInputFormat()).isEqualTo(OrcInputFormat.class.getName());
        assertThat(hiveTable.getSd().getOutputFormat()).isEqualTo(OrcOutputFormat.class.getName());
    }

    @Test
    public void testInsert() throws Exception {
        // src table
        tableEnv.executeSql("create table src (x int,y string)");
        tableEnv.executeSql("insert into src values (1,'a'),(2,'b'),(3,'c')").await();

        // non-partitioned dest table
        tableEnv.executeSql("create table dest (x int)");
        tableEnv.executeSql("insert into dest select x from src").await();
        List<Row> results = queryResult(tableEnv.sqlQuery("select * from dest"));
        assertThat(results.toString()).isEqualTo("[+I[1], +I[2], +I[3]]");
        tableEnv.executeSql("insert overwrite dest values (3),(4),(5)").await();
        results = queryResult(tableEnv.sqlQuery("select * from dest"));
        assertThat(results.toString()).isEqualTo("[+I[3], +I[4], +I[5]]");

        // partitioned dest table
        tableEnv.executeSql("create table dest2 (x int) partitioned by (p1 int,p2 string)");
        tableEnv.executeSql("insert into dest2 partition (p1=0,p2='static') select x from src")
                .await();
        results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
        assertThat(results.toString())
                .isEqualTo("[+I[1, 0, static], +I[2, 0, static], +I[3, 0, static]]");
        tableEnv.executeSql("insert into dest2 partition (p1=1,p2) select x,y from src").await();
        results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
        assertThat(results.toString())
                .isEqualTo(
                        "[+I[1, 0, static], +I[1, 1, a], +I[2, 0, static], +I[2, 1, b], +I[3, 0, static], +I[3, 1, c]]");
        tableEnv.executeSql("insert overwrite dest2 partition (p1,p2) select 1,x,y from src")
                .await();
        results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
        assertThat(results.toString())
                .isEqualTo(
                        "[+I[1, 0, static], +I[1, 1, a], +I[1, 2, b], +I[1, 3, c], +I[2, 0, static], +I[2, 1, b], +I[3, 0, static], +I[3, 1, c]]");

        // test table whose name begins with digits
        tableEnv.executeSql("create table 2t(3c int)");
        tableEnv.executeSql("insert into 2t select x from src").await();
        results = queryResult(tableEnv.sqlQuery("select * from 2t"));
        assertThat(results.toString()).isEqualTo("[+I[1], +I[2], +I[3]]");
    }

    @Test
    public void testAlterTable() throws Exception {
        tableEnv.executeSql("create table tbl (x int) tblproperties('k1'='v1')");
        tableEnv.executeSql("alter table tbl rename to tbl1");

        ObjectPath tablePath = new ObjectPath("default", "tbl1");

        // change properties
        tableEnv.executeSql("alter table `default`.tbl1 set tblproperties ('k2'='v2')");
        Table hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertThat(hiveTable.getParameters().get("k1")).isEqualTo("v1");
        assertThat(hiveTable.getParameters().get("k2")).isEqualTo("v2");

        // change location
        String newLocation = warehouse + "/tbl1_new_location";
        tableEnv.executeSql(
                String.format("alter table default.tbl1 set location '%s'", newLocation));
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertThat(locationPath(hiveTable.getSd().getLocation())).isEqualTo(newLocation);

        // change file format
        tableEnv.executeSql("alter table tbl1 set fileformat orc");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertThat(hiveTable.getSd().getSerdeInfo().getSerializationLib())
                .isEqualTo(OrcSerde.class.getName());
        assertThat(hiveTable.getSd().getInputFormat()).isEqualTo(OrcInputFormat.class.getName());
        assertThat(hiveTable.getSd().getOutputFormat()).isEqualTo(OrcOutputFormat.class.getName());

        // change serde
        tableEnv.executeSql(
                String.format(
                        "alter table tbl1 set serde '%s' with serdeproperties('%s'='%s')",
                        LazyBinarySerDe.class.getName(), serdeConstants.FIELD_DELIM, "\u0001"));
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertThat(hiveTable.getSd().getSerdeInfo().getSerializationLib())
                .isEqualTo(LazyBinarySerDe.class.getName());
        assertThat(hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.FIELD_DELIM))
                .isEqualTo("\u0001");

        // replace columns
        tableEnv.executeSql(
                "alter table tbl1 replace columns (t tinyint,s smallint,i int,b bigint,f float,d double,num decimal,"
                        + "ts timestamp,dt date,str string,var varchar(10),ch char(123),bool boolean,bin binary)");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertThat(hiveTable.getSd().getColsSize()).isEqualTo(14);
        assertThat(hiveTable.getSd().getCols().get(10).getType()).isEqualTo("varchar(10)");
        assertThat(hiveTable.getSd().getCols().get(11).getType()).isEqualTo("char(123)");

        tableEnv.executeSql(
                "alter table tbl1 replace columns (a array<array<int>>,s struct<f1:struct<f11:int,f12:binary>, f2:map<double,date>>,"
                        + "m map<char(5),map<timestamp,decimal(20,10)>>)");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertThat(hiveTable.getSd().getCols().get(0).getType()).isEqualTo("array<array<int>>");
        assertThat(hiveTable.getSd().getCols().get(1).getType())
                .isEqualTo("struct<f1:struct<f11:int,f12:binary>,f2:map<double,date>>");
        assertThat(hiveTable.getSd().getCols().get(2).getType())
                .isEqualTo("map<char(5),map<timestamp,decimal(20,10)>>");

        // add columns
        tableEnv.executeSql("alter table tbl1 add columns (x int,y int)");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertThat(hiveTable.getSd().getColsSize()).isEqualTo(5);

        // change column
        tableEnv.executeSql("alter table tbl1 change column x x1 string comment 'new x col'");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertThat(hiveTable.getSd().getColsSize()).isEqualTo(5);
        FieldSchema newField = hiveTable.getSd().getCols().get(3);
        assertThat(newField.getName()).isEqualTo("x1");
        assertThat(newField.getType()).isEqualTo("string");

        tableEnv.executeSql("alter table tbl1 change column y y int first");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        newField = hiveTable.getSd().getCols().get(0);
        assertThat(newField.getName()).isEqualTo("y");
        assertThat(newField.getType()).isEqualTo("int");

        tableEnv.executeSql("alter table tbl1 change column x1 x2 timestamp after y");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        newField = hiveTable.getSd().getCols().get(1);
        assertThat(newField.getName()).isEqualTo("x2");
        assertThat(newField.getType()).isEqualTo("timestamp");

        // add/replace columns cascade
        tableEnv.executeSql("create table tbl2 (x int) partitioned by (dt date,id bigint)");
        tableEnv.executeSql(
                "alter table tbl2 add partition (dt='2020-01-23',id=1) partition (dt='2020-04-24',id=2)");
        CatalogPartitionSpec partitionSpec1 =
                new CatalogPartitionSpec(
                        new LinkedHashMap<String, String>() {
                            {
                                put("dt", "2020-01-23");
                                put("id", "1");
                            }
                        });
        CatalogPartitionSpec partitionSpec2 =
                new CatalogPartitionSpec(
                        new LinkedHashMap<String, String>() {
                            {
                                put("dt", "2020-04-24");
                                put("id", "2");
                            }
                        });
        tableEnv.executeSql("alter table tbl2 replace columns (ti tinyint,d decimal) cascade");
        ObjectPath tablePath2 = new ObjectPath("default", "tbl2");
        hiveTable = hiveCatalog.getHiveTable(tablePath2);
        Partition hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec1);
        assertThat(hivePartition.getSd().getColsSize()).isEqualTo(2);
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec2);
        assertThat(hivePartition.getSd().getColsSize()).isEqualTo(2);

        tableEnv.executeSql("alter table tbl2 add columns (ch char(5),vch varchar(9)) cascade");
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec1);
        assertThat(hivePartition.getSd().getColsSize()).isEqualTo(4);
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec2);
        assertThat(hivePartition.getSd().getColsSize()).isEqualTo(4);

        // change column cascade
        tableEnv.executeSql("alter table tbl2 change column ch ch char(10) cascade");
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec1);
        assertThat(hivePartition.getSd().getCols().get(2).getType()).isEqualTo("char(10)");
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec2);
        assertThat(hivePartition.getSd().getCols().get(2).getType()).isEqualTo("char(10)");

        tableEnv.executeSql("alter table tbl2 change column vch str string first cascade");
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec1);
        assertThat(hivePartition.getSd().getCols().get(0).getName()).isEqualTo("str");
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec2);
        assertThat(hivePartition.getSd().getCols().get(0).getName()).isEqualTo("str");
    }

    @Test
    public void testAlterPartition() throws Exception {
        tableEnv.executeSql(
                "create table tbl (x tinyint,y string) partitioned by (p1 bigint,p2 date)");
        tableEnv.executeSql(
                "alter table tbl add partition (p1=1000,p2='2020-05-01') partition (p1=2000,p2='2020-01-01')");
        CatalogPartitionSpec spec1 =
                new CatalogPartitionSpec(
                        new LinkedHashMap<String, String>() {
                            {
                                put("p1", "1000");
                                put("p2", "2020-05-01");
                            }
                        });
        CatalogPartitionSpec spec2 =
                new CatalogPartitionSpec(
                        new LinkedHashMap<String, String>() {
                            {
                                put("p1", "2000");
                                put("p2", "2020-01-01");
                            }
                        });
        ObjectPath tablePath = new ObjectPath("default", "tbl");

        Table hiveTable = hiveCatalog.getHiveTable(tablePath);

        // change location
        String location = warehouse + "/new_part_location";
        tableEnv.executeSql(
                String.format(
                        "alter table tbl partition (p1=1000,p2='2020-05-01') set location '%s'",
                        location));
        Partition partition = hiveCatalog.getHivePartition(hiveTable, spec1);
        assertThat(locationPath(partition.getSd().getLocation())).isEqualTo(location);

        // change file format
        tableEnv.executeSql(
                "alter table tbl partition (p1=2000,p2='2020-01-01') set fileformat rcfile");
        partition = hiveCatalog.getHivePartition(hiveTable, spec2);
        assertThat(partition.getSd().getSerdeInfo().getSerializationLib())
                .isEqualTo(LazyBinaryColumnarSerDe.class.getName());
        assertThat(partition.getSd().getInputFormat()).isEqualTo(RCFileInputFormat.class.getName());
        assertThat(partition.getSd().getOutputFormat())
                .isEqualTo(RCFileOutputFormat.class.getName());

        // change serde
        tableEnv.executeSql(
                String.format(
                        "alter table tbl partition (p1=1000,p2='2020-05-01') set serde '%s' with serdeproperties('%s'='%s')",
                        LazyBinarySerDe.class.getName(), serdeConstants.LINE_DELIM, "\n"));
        partition = hiveCatalog.getHivePartition(hiveTable, spec1);
        assertThat(partition.getSd().getSerdeInfo().getSerializationLib())
                .isEqualTo(LazyBinarySerDe.class.getName());
        assertThat(partition.getSd().getSerdeInfo().getParameters().get(serdeConstants.LINE_DELIM))
                .isEqualTo("\n");
    }

    @Test
    public void testView() throws Exception {
        tableEnv.executeSql("create table tbl (x int,y string)");

        // create
        tableEnv.executeSql(
                "create view v(vx) comment 'v comment' tblproperties ('k1'='v1') as select x from tbl");
        ObjectPath viewPath = new ObjectPath("default", "v");
        CatalogBaseTable catalogBaseTable = hiveCatalog.getTable(viewPath);
        assertThat(catalogBaseTable).isInstanceOf(CatalogView.class);
        assertThat(catalogBaseTable.getUnresolvedSchema().getColumns().get(0).getName())
                .isEqualTo("vx");
        assertThat(catalogBaseTable.getOptions().get("k1")).isEqualTo("v1");

        // change properties
        tableEnv.executeSql("alter view v set tblproperties ('k1'='v11')");
        catalogBaseTable = hiveCatalog.getTable(viewPath);
        assertThat(catalogBaseTable.getOptions().get("k1")).isEqualTo("v11");

        // change query
        tableEnv.executeSql("alter view v as select y from tbl");
        catalogBaseTable = hiveCatalog.getTable(viewPath);
        assertThat(catalogBaseTable.getUnresolvedSchema().getColumns().get(0).getName())
                .isEqualTo("y");

        // rename
        tableEnv.executeSql("alter view v rename to v1");
        viewPath = new ObjectPath("default", "v1");
        assertThat(hiveCatalog.tableExists(viewPath)).isTrue();

        // drop
        tableEnv.executeSql("drop view v1");
        assertThat(hiveCatalog.tableExists(viewPath)).isFalse();
    }

    @Test
    public void testFunction() throws Exception {
        // create function
        tableEnv.executeSql(
                String.format(
                        "create function default.my_abs as '%s'", GenericUDFAbs.class.getName()));
        List<Row> functions =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show functions").collect());
        assertThat(functions.toString()).contains("my_abs");
        // call the function
        tableEnv.executeSql("create table src(x int)");
        tableEnv.executeSql("insert into src values (1),(-1)").await();
        assertThat(queryResult(tableEnv.sqlQuery("select my_abs(x) from src")).toString())
                .isEqualTo("[+I[1], +I[1]]");
        // drop the function
        tableEnv.executeSql("drop function my_abs");
        assertThat(hiveCatalog.functionExists(new ObjectPath("default", "my_abs"))).isFalse();
        tableEnv.executeSql("drop function if exists foo");
    }

    @Test
    public void testTemporaryFunction() throws Exception {
        // create temp function
        tableEnv.executeSql(
                String.format(
                        "create temporary function temp_abs as '%s'",
                        GenericUDFAbs.class.getName()));
        String[] functions = tableEnv.listUserDefinedFunctions();
        assertThat(functions).isEqualTo(new String[] {"temp_abs"});
        // call the function
        tableEnv.executeSql("create table src(x int)");
        tableEnv.executeSql("insert into src values (1),(-1)").await();
        assertThat(queryResult(tableEnv.sqlQuery("select temp_abs(x) from src")).toString())
                .isEqualTo("[+I[1], +I[1]]");
        // switch DB and the temp function can still be used
        tableEnv.executeSql("create database db1");
        tableEnv.useDatabase("db1");
        assertThat(
                        queryResult(tableEnv.sqlQuery("select temp_abs(x) from `default`.src"))
                                .toString())
                .isEqualTo("[+I[1], +I[1]]");
        // drop the function
        tableEnv.executeSql("drop temporary function temp_abs");
        functions = tableEnv.listUserDefinedFunctions();
        assertThat(functions).isEmpty();
        tableEnv.executeSql("drop temporary function if exists foo");
    }

    @Test
    public void testTemporaryFunctionUDAF() throws Exception {
        // create temp function
        tableEnv.executeSql(
                String.format(
                        "create temporary function temp_count as '%s'",
                        GenericUDAFCount.class.getName()));
        String[] functions = tableEnv.listUserDefinedFunctions();
        assertThat(functions).isEqualTo(new String[] {"temp_count"});
        // call the function
        tableEnv.executeSql("create table src(x int)");
        tableEnv.executeSql("insert into src values (1),(-1)").await();
        assertThat(queryResult(tableEnv.sqlQuery("select temp_count(x) from src")).toString())
                .isEqualTo("[+I[2]]");
        // switch DB and the temp function can still be used
        tableEnv.executeSql("create database db1");
        tableEnv.useDatabase("db1");
        assertThat(
                        queryResult(tableEnv.sqlQuery("select temp_count(x) from `default`.src"))
                                .toString())
                .isEqualTo("[+I[2]]");
        // drop the function
        tableEnv.executeSql("drop temporary function temp_count");
        functions = tableEnv.listUserDefinedFunctions();
        assertThat(functions).isEmpty();
        tableEnv.executeSql("drop temporary function if exists foo");
    }

    @Test
    public void testCatalog() {
        List<Row> catalogs =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show catalogs").collect());
        assertThat(catalogs).hasSize(2);
        tableEnv.executeSql("use catalog " + CatalogManagerMocks.DEFAULT_CATALOG);
        List<Row> databases =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show databases").collect());
        assertThat(databases).hasSize(1);
        assertThat(databases.get(0).toString())
                .isEqualTo("+I[" + CatalogManagerMocks.DEFAULT_DATABASE + "]");
        String catalogName =
                tableEnv.executeSql("show current catalog").collect().next().toString();
        assertThat(catalogName).isEqualTo("+I[" + CatalogManagerMocks.DEFAULT_CATALOG + "]");
        String databaseName =
                tableEnv.executeSql("show current database").collect().next().toString();
        assertThat(databaseName).isEqualTo("+I[" + CatalogManagerMocks.DEFAULT_DATABASE + "]");
    }

    @Test
    public void testAddDropPartitions() throws Exception {
        tableEnv.executeSql(
                "create table tbl (x int,y binary) partitioned by (dt date,country string)");
        tableEnv.executeSql(
                "alter table tbl add partition (dt='2020-04-30',country='china') partition (dt='2020-04-30',country='us')");

        ObjectPath tablePath = new ObjectPath("default", "tbl");
        assertThat(hiveCatalog.listPartitions(tablePath)).hasSize(2);

        String partLocation = warehouse + "/part3_location";
        tableEnv.executeSql(
                String.format(
                        "alter table tbl add partition (dt='2020-05-01',country='belgium') location '%s'",
                        partLocation));
        Table hiveTable = hiveCatalog.getHiveTable(tablePath);
        CatalogPartitionSpec spec =
                new CatalogPartitionSpec(
                        new LinkedHashMap<String, String>() {
                            {
                                put("dt", "2020-05-01");
                                put("country", "belgium");
                            }
                        });
        Partition hivePartition = hiveCatalog.getHivePartition(hiveTable, spec);
        assertThat(locationPath(hivePartition.getSd().getLocation())).isEqualTo(partLocation);

        tableEnv.executeSql(
                "alter table tbl drop partition (dt='2020-04-30',country='china'),partition (dt='2020-05-01',country='belgium')");
        assertThat(hiveCatalog.listPartitions(tablePath)).hasSize(1);
    }

    @Test
    public void testShowPartitions() throws Exception {
        tableEnv.executeSql(
                "create table tbl (x int,y binary) partitioned by (dt date, country string)");
        tableEnv.executeSql(
                "alter table tbl add partition (dt='2020-04-30',country='china') partition (dt='2020-04-30',country='us')");

        ObjectPath tablePath = new ObjectPath("default", "tbl");
        assertThat(hiveCatalog.listPartitions(tablePath)).hasSize(2);

        List<Row> partitions =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show partitions tbl").collect());
        assertThat(partitions).hasSize(2);
        assertThat(partitions.toString()).contains("dt=2020-04-30/country=china");
        assertThat(partitions.toString()).contains("dt=2020-04-30/country=us");
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("show partitions tbl partition (dt='2020-04-30')")
                                .collect());
        assertThat(partitions).hasSize(2);
        assertThat(partitions.toString()).contains("dt=2020-04-30/country=china");
        assertThat(partitions.toString()).contains("dt=2020-04-30/country=us");
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("show partitions tbl partition (country='china')")
                                .collect());
        assertThat(partitions).hasSize(1);
        assertThat(partitions.toString()).contains("dt=2020-04-30/country=china");
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-04-30',country='china')")
                                .collect());
        assertThat(partitions).hasSize(1);
        assertThat(partitions.toString()).contains("dt=2020-04-30/country=china");
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-05-01',country='japan')")
                                .collect());
        assertThat(partitions).isEmpty();

        assertThatThrownBy(
                        () ->
                                CollectionUtil.iteratorToList(
                                        tableEnv.executeSql(
                                                        "show partitions tbl partition (de='2020-04-30',city='china')")
                                                .collect()))
                .isInstanceOf(TableException.class)
                .hasMessage(
                        String.format(
                                "Could not execute SHOW PARTITIONS %s.%s PARTITION (de=2020-04-30, city=china)",
                                hiveCatalog.getName(), tablePath));

        tableEnv.executeSql(
                "alter table tbl drop partition (dt='2020-04-30',country='china'),partition (dt='2020-04-30',country='us')");
        assertThat(hiveCatalog.listPartitions(tablePath)).isEmpty();

        tableEnv.executeSql("drop table tbl");
        tableEnv.executeSql(
                "create table tbl (x int,y binary) partitioned by (dt timestamp, country string)");
        tableEnv.executeSql(
                "alter table tbl add partition (dt='2020-04-30 01:02:03',country='china') partition (dt='2020-04-30 04:05:06',country='us')");

        partitions =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show partitions tbl").collect());
        assertThat(partitions).hasSize(2);
        assertThat(partitions.toString()).contains("dt=2020-04-30 01:02:03/country=china");
        assertThat(partitions.toString()).contains("dt=2020-04-30 04:05:06/country=us");
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-04-30 01:02:03')")
                                .collect());
        assertThat(partitions).hasSize(1);
        assertThat(partitions.toString()).contains("dt=2020-04-30 01:02:03/country=china");
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-04-30 04:05:06')")
                                .collect());
        assertThat(partitions).hasSize(1);
        assertThat(partitions.toString()).contains("dt=2020-04-30 04:05:06/country=us");
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-04-30 01:02:03',country='china')")
                                .collect());
        assertThat(partitions).hasSize(1);
        assertThat(partitions.toString()).contains("dt=2020-04-30 01:02:03/country=china");
    }

    @Test
    public void testUnsupportedOperation() {
        List<String> statements =
                Arrays.asList(
                        "create or replace view v as select x from foo",
                        "create materialized view v as select x from foo",
                        "create temporary table foo (x int)",
                        "create table foo (x int) stored by 'handler.class'",
                        "create table foo (x int) clustered by (x) into 3 buckets",
                        "create table foo (x int) skewed by (x) on (1,2,3)",
                        "describe foo partition (p=1)",
                        "describe db.tbl col",
                        "show tables in db1",
                        "show tables like 'tbl*'",
                        "show views in db1",
                        "show views like '*view'");

        for (String statement : statements) {
            verifyUnsupportedOperation(statement);
        }
    }

    private void verifyUnsupportedOperation(String ddl) {
        assertThatThrownBy(() -> tableEnv.executeSql(ddl))
                .isInstanceOf(ValidationException.class)
                .getCause()
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static String locationPath(String locationURI) throws URISyntaxException {
        return new URI(locationURI).getPath();
    }

    private static List<Row> queryResult(org.apache.flink.table.api.Table table) {
        return CollectionUtil.iteratorToList(table.execute().collect());
    }
}
