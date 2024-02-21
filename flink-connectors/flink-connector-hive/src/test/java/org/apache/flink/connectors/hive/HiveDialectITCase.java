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

import org.apache.flink.table.HiveVersionTestUtil;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableResult;
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
import org.apache.flink.table.functions.hive.HiveGenericUDTFTest;
import org.apache.flink.table.functions.hive.util.TestSplitUDTFInitializeWithStructObjectInspector;
import org.apache.flink.table.operations.DescribeTableOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.command.AddJarOperation;
import org.apache.flink.table.operations.command.ClearOperation;
import org.apache.flink.table.operations.command.HelpOperation;
import org.apache.flink.table.operations.command.QuitOperation;
import org.apache.flink.table.operations.command.ResetOperation;
import org.apache.flink.table.operations.command.SetOperation;
import org.apache.flink.table.planner.delegation.hive.HiveParser;
import org.apache.flink.table.planner.delegation.hive.operations.HiveExecutableOperation;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

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
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.catalog.hive.util.Constants.TABLE_LOCATION_URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test Hive syntax when Hive dialect is used. */
public class HiveDialectITCase {

    @ClassRule public static TemporaryFolder tempFolder = new TemporaryFolder();

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
        // execute some sql and verify the parser/operation executor instance is reused
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
        assertThat(hiveTable.getParameters()).doesNotContainKey(TABLE_LOCATION_URI);

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
                (DescribeTableOperation)
                        ((HiveExecutableOperation) parser.parse("desc tbl1").get(0))
                                .getInnerOperation();
        assertThat(operation.isExtended()).isFalse();
        assertThat(operation.getSqlIdentifier())
                .isEqualTo(ObjectIdentifier.of(hiveCatalog.getName(), "default", "tbl1"));

        operation =
                (DescribeTableOperation)
                        ((HiveExecutableOperation) parser.parse("describe default.tbl2").get(0))
                                .getInnerOperation();
        assertThat(operation.isExtended()).isFalse();
        assertThat(operation.getSqlIdentifier())
                .isEqualTo(ObjectIdentifier.of(hiveCatalog.getName(), "default", "tbl2"));

        operation =
                (DescribeTableOperation)
                        ((HiveExecutableOperation) parser.parse("describe extended tbl3").get(0))
                                .getInnerOperation();
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
        Schema schema = catalogTable.getUnresolvedSchema();
        assertThat(schema.getPrimaryKey()).as("PK not present").isPresent();
        assertThat(schema.getPrimaryKey().get().getColumnNames().size()).isEqualTo(1);
        assertThat(schema.getPrimaryKey().get().getConstraintName()).isEqualTo("pk_name");
        List<Schema.UnresolvedColumn> columns = schema.getColumns();
        assertThat(HiveTestUtils.getType(columns.get(0)).getLogicalType().isNullable())
                .as("PK cannot be null")
                .isFalse();
        assertThat(HiveTestUtils.getType(columns.get(1)).getLogicalType().isNullable())
                .as("RELY NOT NULL should be reflected in schema")
                .isFalse();
        assertThat(HiveTestUtils.getType(columns.get(2)).getLogicalType().isNullable())
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
        tableEnv.executeSql("insert overwrite table dest values (3),(4),(5)").await();
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
        tableEnv.executeSql("insert overwrite table dest2 partition (p1,p2) select 1,x,y from src")
                .await();
        results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
        assertThat(results.toString())
                .isEqualTo(
                        "[+I[1, 0, static], +I[1, 1, a], +I[1, 2, b], +I[1, 3, c], +I[2, 0, static], +I[2, 1, b], +I[3, 0, static], +I[3, 1, c]]");
        tableEnv.executeSql(
                        "insert overwrite table default.dest2 partition (p1=1,p2='static') if not exists select x from src")
                .await();
        results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
        assertThat(results.toString())
                .isEqualTo(
                        "[+I[1, 0, static], +I[1, 1, a], +I[1, 1, static], +I[1, 2, b], +I[1, 3, c], +I[2, 0, static],"
                                + " +I[2, 1, b], +I[2, 1, static], +I[3, 0, static], +I[3, 1, c], +I[3, 1, static]]");

        // test table partitioned by decimal type
        tableEnv.executeSql(
                "create table dest3 (key int, value string) partitioned by (p1 decimal(5, 2)) ");
        tableEnv.executeSql(
                        "insert overwrite table dest3 partition (p1) select 1,y,100.45 from src")
                .await();
        results = queryResult(tableEnv.sqlQuery("select * from dest3"));
        assertThat(results.toString())
                .isEqualTo("[+I[1, a, 100.45], +I[1, b, 100.45], +I[1, c, 100.45]]");
    }

    @Test
    public void testInsertOverwrite() throws Exception {
        tableEnv.executeSql("create table T1(a int, b string)");
        tableEnv.executeSql("insert into T1 values(1, 'v1')").await();
        tableEnv.executeSql("create table T2(a int, b string) partitioned by (dt string)");
        tableEnv.executeSql(
                        "insert overwrite table default.T2 partition (dt = '2023-01-01') select * from default.T1")
                .await();
        List<Row> rows = queryResult(tableEnv.sqlQuery("select * from T2"));
        assertThat(rows.toString()).isEqualTo("[+I[1, v1, 2023-01-01]]");
        tableEnv.executeSql(
                        "insert overwrite table default.T2 partition (dt = '2023-01-01') select * from default.T1")
                .await();
        rows = queryResult(tableEnv.sqlQuery("select * from T2"));
        assertThat(rows.toString()).isEqualTo("[+I[1, v1, 2023-01-01]]");
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
    public void testTableWithSubDirsInPartitionDir() throws Exception {
        tableEnv.executeSql("CREATE TABLE fact_tz(x int) PARTITIONED BY (ds STRING, hr STRING)");
        tableEnv.executeSql("INSERT OVERWRITE TABLE fact_tz PARTITION (ds='1', hr='1') select 1")
                .await();
        tableEnv.executeSql("INSERT OVERWRITE TABLE fact_tz PARTITION (ds='1', hr='2') select 2")
                .await();
        String location = warehouse + "/fact_tz";
        // create an external table
        tableEnv.executeSql(
                String.format(
                        "create external table fact_daily(x int) PARTITIONED BY (ds STRING) location '%s'",
                        location));
        tableEnv.executeSql(
                String.format(
                        "ALTER TABLE fact_daily ADD PARTITION (ds='1') location '%s'",
                        location + "/ds=1"));
        List<Row> results =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select * from fact_daily WHERE ds='1' order by x")
                                .collect());
        // the data read from the external table fact_daily should contain the data in
        // directory 'ds=1/hr=1', 'ds=1/hr=2'
        assertThat(results.toString()).isEqualTo("[+I[1, 1], +I[2, 1]]");

        tableEnv.getConfig()
                .set(
                        HiveOptions.TABLE_EXEC_HIVE_READ_PARTITION_WITH_SUBDIRECTORY_ENABLED.key(),
                        "false");
        // should throw exception when disable reading sub-dirs in partition directory
        assertThatThrownBy(
                        () ->
                                CollectionUtil.iteratorToList(
                                        tableEnv.executeSql("select * from fact_daily WHERE ds='1'")
                                                .collect()))
                .satisfiesAnyOf(
                        anyCauseMatches(
                                String.format(
                                        "Not a file: file:%s", warehouse + "/fact_tz/ds=1/hr=1")),
                        anyCauseMatches(
                                String.format(
                                        "Not a file: file:%s", warehouse + "/fact_tz/ds=1/hr=2")));
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
    public void testCreateFunctionUsingJar() throws Exception {
        tableEnv.executeSql("create table src(x int)");
        tableEnv.executeSql("insert into src values (1), (2)").await();
        String udfCodeTemplate =
                "public class %s"
                        + " extends org.apache.hadoop.hive.ql.exec.UDF {\n"
                        + " public int evaluate(int content) {\n"
                        + "    return content + 1;\n"
                        + " }"
                        + "}\n";
        String udfClass = "addOne";
        String udfCode = String.format(udfCodeTemplate, udfClass);
        File jarFile =
                UserClassLoaderJarTestUtils.createJarFile(
                        tempFolder.newFolder("test-jar"), "test-udf.jar", udfClass, udfCode);
        // test create function using jar
        tableEnv.executeSql(
                String.format(
                        "create function add_one as '%s' using jar '%s'",
                        udfClass, jarFile.getPath()));
        assertThat(
                        CollectionUtil.iteratorToList(
                                        tableEnv.executeSql("select add_one(x) from src").collect())
                                .toString())
                .isEqualTo("[+I[2], +I[3]]");

        // test create temporary function using jar
        // create a new jarfile with a new class name
        udfClass = "addOne1";
        udfCode = String.format(udfCodeTemplate, udfClass);
        jarFile =
                UserClassLoaderJarTestUtils.createJarFile(
                        tempFolder.newFolder("test-jar-1"), "test-udf-1.jar", udfClass, udfCode);
        tableEnv.executeSql(
                String.format(
                        "create temporary function t_add_one as '%s' using jar '%s'",
                        udfClass, jarFile.getPath()));
        assertThat(
                        CollectionUtil.iteratorToList(
                                        tableEnv.executeSql("select t_add_one(x) from src")
                                                .collect())
                                .toString())
                .isEqualTo("[+I[2], +I[3]]");
    }

    @Test
    public void testTemporaryFunctionUDTF() throws Exception {
        // function initialize with ObjectInspector
        tableEnv.executeSql(
                String.format(
                        "create temporary function temp_split_obj_inspector as '%s'",
                        HiveGenericUDTFTest.TestSplitUDTF.class.getName()));
        // function initialize with StructObjectInspector
        tableEnv.executeSql(
                String.format(
                        "create temporary function temp_split_struct_obj_inspector as '%s'",
                        TestSplitUDTFInitializeWithStructObjectInspector.class.getName()));
        String[] functions = tableEnv.listUserDefinedFunctions();
        assertThat(functions)
                .isEqualTo(
                        new String[] {
                            "temp_split_obj_inspector", "temp_split_struct_obj_inspector"
                        });
        // call the function
        tableEnv.executeSql("create table src(x string)");
        tableEnv.executeSql("insert into src values ('a,b,c')").await();
        assertThat(
                        queryResult(
                                        tableEnv.sqlQuery(
                                                "select temp_split_obj_inspector(x) from src"))
                                .toString())
                .isEqualTo("[+I[a], +I[b], +I[c]]");
        assertThat(
                        queryResult(
                                        tableEnv.sqlQuery(
                                                "select temp_split_struct_obj_inspector(x) from src"))
                                .toString())
                .isEqualTo("[+I[a], +I[b], +I[c]]");
        // switch DB and the temp function can still be used
        tableEnv.executeSql("create database db1");
        tableEnv.useDatabase("db1");
        assertThat(
                        queryResult(
                                        tableEnv.sqlQuery(
                                                "select temp_split_obj_inspector(x) from `default`.src"))
                                .toString())
                .isEqualTo("[+I[a], +I[b], +I[c]]");
        assertThat(
                        queryResult(
                                        tableEnv.sqlQuery(
                                                "select temp_split_struct_obj_inspector(x) from `default`.src"))
                                .toString())
                .isEqualTo("[+I[a], +I[b], +I[c]]");
        // drop the function
        tableEnv.executeSql("drop temporary function temp_split_obj_inspector");
        tableEnv.executeSql("drop temporary function temp_split_struct_obj_inspector");
        functions = tableEnv.listUserDefinedFunctions();
        assertThat(functions.length).isEqualTo(0);
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

        // set a deterministic default partition name
        tableEnv.executeSql("set hiveconf:hive.exec.default.partition.name=_DEFAULT_");
        // show partitions for the table containing default partition
        tableEnv.executeSql("create table tb1 (a string) partitioned by (c int)");
        tableEnv.executeSql(
                        "INSERT OVERWRITE TABLE tb1 PARTITION (c) values ('Col1', null), ('Col1', 5)")
                .await();
        // show partitions should include the null partition
        partitions =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show partitions tb1").collect());
        assertThat(partitions.toString()).isEqualTo("[+I[c=5], +I[c=_DEFAULT_]]");
        // show specific null partition
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("show partitions tb1 partition (c='_DEFAULT_')")
                                .collect());
        assertThat(partitions.toString()).isEqualTo("[+I[c=_DEFAULT_]]");
        // drop null partition should also work
        tableEnv.executeSql("ALTER TABLE tb1 DROP PARTITION (c='_DEFAULT_')");
        partitions =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show partitions tb1").collect());
        assertThat(partitions.toString()).isEqualTo("[+I[c=5]]");
    }

    @Test
    public void testMacro() throws Exception {
        tableEnv.executeSql("create temporary macro string_len (x string) length(x)");
        tableEnv.executeSql("create temporary macro string_len_plus(x string) length(x) + 1");
        tableEnv.executeSql("create table macro_test (x string)");
        tableEnv.executeSql("insert into table macro_test values ('bb'), ('a'), ('cc')").await();
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "select string_len(x), string_len_plus(x) from macro_test")
                                .collect());
        assertThat(result.toString()).isEqualTo("[+I[2, 3], +I[1, 2], +I[2, 3]]");
        // drop macro
        tableEnv.executeSql("drop temporary macro string_len_plus");
        // create macro
        tableEnv.executeSql("create temporary macro string_len_plus(x string) length(x) + 2");
        result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "select string_len(x), string_len_plus(x) from macro_test")
                                .collect());
        assertThat(result.toString()).isEqualTo("[+I[2, 4], +I[1, 3], +I[2, 4]]");
        String badMacroName = "db.string_len";
        // should fail when create macro whose name contains "."
        assertThatThrownBy(
                        () ->
                                tableEnv.executeSql(
                                        String.format(
                                                "create temporary macro `%s` (x string) length(x)",
                                                badMacroName)))
                .hasRootCauseInstanceOf(SemanticException.class)
                .hasRootCauseMessage(
                        String.format(
                                "CREATE TEMPORARY MACRO doesn't allow \".\" character in the macro name, but the name is \"%s\".",
                                badMacroName));
        // should fail when drop macro whose name contains "."
        assertThatThrownBy(
                        () ->
                                tableEnv.executeSql(
                                        String.format("drop temporary macro `%s`", badMacroName)))
                .hasRootCauseInstanceOf(SemanticException.class)
                .hasRootCauseMessage(
                        "DROP TEMPORARY MACRO doesn't allow \".\" character in the macro name, but the name is \"%s\".",
                        badMacroName);
    }

    @Test
    public void testSetCommand() throws Exception {
        // test set system:
        tableEnv.executeSql("set system:xxx=5");
        assertThat(System.getProperty("xxx")).isEqualTo("5");
        List<Row> result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select '${system:xxx}'").collect());
        assertThat(result.toString()).isEqualTo("[+I[5]]");

        // test set hiveconf:
        tableEnv.executeSql("set hiveconf:yyy=${system:xxx}");
        assertThat(hiveCatalog.getHiveConf().get("yyy")).isEqualTo("5");
        // disable variable substitute
        tableEnv.executeSql("set hiveconf:hive.variable.substitute=false");
        result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select '${hiveconf:yyy}'").collect());
        assertThat(result.toString()).isEqualTo("[+I[${hiveconf:yyy}]]");
        // enable variable substitute again
        tableEnv.executeSql("set hiveconf:hive.variable.substitute=true");
        result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select ${hiveconf:yyy}").collect());
        assertThat(result.toString()).isEqualTo("[+I[5]]");

        // test set hivevar:
        tableEnv.executeSql("set hivevar:a=1");
        tableEnv.executeSql("set hiveconf:zzz=${hivevar:a}");
        assertThat(hiveCatalog.getHiveConf().get("zzz")).isEqualTo("1");

        // test set nested variables
        tableEnv.executeSql("set hiveconf:b=a");
        tableEnv.executeSql("set system:c=${hivevar:${hiveconf:b}}");
        assertThat(System.getProperty("c")).isEqualTo("1");
        result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select ${hivevar:${hiveconf:b}}").collect());
        assertThat(result.toString()).isEqualTo("[+I[1]]");

        // test the hivevar still exists when we renew the sql parser
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql("show tables");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("set hiveconf:zzz1=${hivevar:a}");
        assertThat(hiveCatalog.getHiveConf().get("zzz1")).isEqualTo("1");
        result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select ${hiveconf:zzz1}").collect());
        assertThat(result.toString()).isEqualTo("[+I[1]]");

        // test set metaconf:
        tableEnv.executeSql("set metaconf:hive.metastore.try.direct.sql=false");
        Hive hive = Hive.get(hiveCatalog.getHiveConf());
        assertThat(hive.getMetaConf("hive.metastore.try.direct.sql")).isEqualTo("false");

        // test 'set xxx = xxx' should be pared as Flink SetOperation
        TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
        Parser parser = tableEnvInternal.getParser();
        Operation operation = parser.parse("set user.name=hive_test_user").get(0);
        assertThat(operation).isInstanceOf(SetOperation.class);
        assertThat(operation.asSummaryString()).isEqualTo("SET user.name=hive_test_user");

        // test 'set xxx='
        tableEnv.executeSql("set user.name=");
        assertThat(hiveCatalog.getHiveConf().get("user.name")).isEmpty();

        // test 'set xxx'
        List<Row> rows =
                CollectionUtil.iteratorToList(tableEnv.executeSql("set system:xxx").collect());
        assertThat(rows.toString()).isEqualTo("[+I[system:xxx=5]]");

        // test 'set -v'
        rows = CollectionUtil.iteratorToList(tableEnv.executeSql("set -v").collect());
        assertThat(rows.toString())
                .contains("system:xxx=5")
                .contains("env:PATH=" + System.getenv("PATH"))
                .contains("hivevar:a=1")
                .contains("hiveconf:fs.defaultFS=file:///")
                .contains("execution.runtime-mode=BATCH");

        // test set env isn't supported
        assertThatThrownBy(() -> tableEnv.executeSql("set env:xxx=yyy"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("env:* variables can not be set.");
        // test substitution for env variable in sql
        String path = System.getenv("PATH");
        result =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("select '${env:PATH}'").collect());
        assertThat(result.toString()).isEqualTo(String.format("[+I[%s]]", path));
    }

    @Test
    public void testAddCommand() {
        TableEnvironmentInternal tableEnvInternal = (TableEnvironmentInternal) tableEnv;
        Parser parser = tableEnvInternal.getParser();

        // test add jar
        Operation operation = parser.parse("add jar test.jar").get(0);
        assertThat(operation).isInstanceOf(AddJarOperation.class);
        assertThat(((AddJarOperation) operation).getPath()).isEqualTo("test.jar");
        // test add jar with variable substitute
        operation = parser.parse("add jar ${hiveconf:common-key}.jar").get(0);
        assertThat(operation).isInstanceOf(AddJarOperation.class);
        assertThat(((AddJarOperation) operation).getPath()).isEqualTo("common-val.jar");

        // test unsupported add command
        assertThatThrownBy(() -> tableEnv.executeSql("add jar t1.jar t2.jar"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        "Add multiple jar in one single statement is not supported yet. Usage: ADD JAR <file_path>");
        assertThatThrownBy(() -> tableEnv.executeSql("add file t1.txt"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("ADD FILE is not supported yet. Usage: ADD JAR <file_path>");
        assertThatThrownBy(() -> tableEnv.executeSql("add archive t1.tgz"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("ADD ARCHIVE is not supported yet. Usage: ADD JAR <file_path>");
    }

    @Test
    public void testShowCreateTable() throws Exception {
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql(
                "create table t1(id BIGINT,\n"
                        + "  name STRING) WITH (\n"
                        + "  'connector' = 'datagen' "
                        + ")");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql(
                "create table t2 (key string, value string) comment 'show create table' partitioned by (a string, b int)"
                        + " tblproperties ('k1' = 'v1')");

        // should throw exception for show non-hive table
        assertThatThrownBy(() -> tableEnv.executeSql("show create table t1"))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage(
                        String.format(
                                "The table %s to show isn't a Hive table,"
                                        + " but 'SHOW CREATE TABLE' only supports Hive table currently.",
                                "default.t1"));

        // show hive table
        TableResult showCreateTableT2 = tableEnv.executeSql("show create table t2");
        assertThat(showCreateTableT2.getResultKind()).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT);
        String actualResult =
                (String)
                        CollectionUtil.iteratorToList(showCreateTableT2.collect())
                                .get(0)
                                .getField(0);
        Table table = hiveCatalog.getHiveTable(new ObjectPath("default", "t2"));
        String expectLastDdlTime = table.getParameters().get("transient_lastDdlTime");
        String expectedTableProperties =
                String.format(
                        "%s  'k1'='v1', \n  'transient_lastDdlTime'='%s'",
                        // if it's hive 3.x, table properties should also contain
                        // 'bucketing_version'='2'
                        HiveVersionTestUtil.HIVE_310_OR_LATER
                                ? "  'bucketing_version'='2', \n"
                                : "",
                        expectLastDdlTime);
        String expectedResult =
                String.format(
                        "CREATE TABLE `default.t2`(\n"
                                + "  `key` string, \n"
                                + "  `value` string)\n"
                                + "COMMENT 'show create table'\n"
                                + "PARTITIONED BY ( \n"
                                + "  `a` string, \n"
                                + "  `b` int)\n"
                                + "ROW FORMAT SERDE \n"
                                + "  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' \n"
                                + "STORED AS INPUTFORMAT \n"
                                + "  'org.apache.hadoop.mapred.TextInputFormat' \n"
                                + "OUTPUTFORMAT \n"
                                + "  'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'\n"
                                + "LOCATION\n"
                                + "  'file:%s'\n"
                                + "TBLPROPERTIES (\n%s)\n",
                        warehouse + "/t2", expectedTableProperties);
        assertThat(actualResult).isEqualTo(expectedResult);
    }

    @Test
    public void testDescribeTable() {
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql(
                "create table t1(id BIGINT,\n"
                        + "  name STRING) WITH (\n"
                        + "  'connector' = 'datagen' "
                        + ")");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        tableEnv.executeSql("create table t2(a int, b string, c boolean)");
        tableEnv.executeSql(
                "create table t3(a decimal(10, 2), b double, c float) partitioned by (d date)");

        // desc non-hive table
        TableResult descT1 = tableEnv.executeSql("desc t1");
        assertThat(descT1.getResultKind()).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT);
        List<Row> result = CollectionUtil.iteratorToList(descT1.collect());
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[id, BIGINT, true, null, null, null], +I[name, STRING, true, null, null, null]]");
        // desc hive table
        TableResult descT2 = tableEnv.executeSql("desc t2");
        assertThat(descT2.getResultKind()).isEqualTo(ResultKind.SUCCESS_WITH_CONTENT);
        result = CollectionUtil.iteratorToList(descT2.collect());
        assertThat(result.toString())
                .isEqualTo("[+I[a, int, ], +I[b, string, ], +I[c, boolean, ]]");
        result = CollectionUtil.iteratorToList(tableEnv.executeSql("desc default.t3").collect());
        assertThat(result.toString())
                .isEqualTo(
                        "[+I[a, decimal(10,2), ], +I[b, double, ], +I[c, float, ], +I[d, date, ],"
                                + " +I[# Partition Information, , ], +I[d, date, ]]");
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
                .cause()
                .isInstanceOf(UnsupportedOperationException.class);
    }

    private static String locationPath(String locationURI) throws URISyntaxException {
        return new URI(locationURI).getPath();
    }

    private static List<Row> queryResult(org.apache.flink.table.api.Table table) {
        return CollectionUtil.iteratorToList(table.execute().collect());
    }
}
