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
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogPropertiesUtil;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.planner.delegation.hive.HiveParser;
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
import java.util.LinkedHashMap;
import java.util.List;

import static org.apache.flink.table.api.EnvironmentSettings.DEFAULT_BUILTIN_CATALOG;
import static org.apache.flink.table.api.EnvironmentSettings.DEFAULT_BUILTIN_DATABASE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

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
        tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
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
        assertTrue(parser instanceof HiveParser);
        // execute some sql and verify the parser instance is reused
        tableEnvInternal.executeSql("show databases");
        assertSame(parser, tableEnvInternal.getParser());
        // switching dialect will result in a new parser
        tableEnvInternal.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        assertNotEquals(
                parser.getClass().getName(), tableEnvInternal.getParser().getClass().getName());
    }

    @Test
    public void testCreateDatabase() throws Exception {
        tableEnv.executeSql("create database db1 comment 'db1 comment'");
        Database db = hiveCatalog.getHiveDatabase("db1");
        assertEquals("db1 comment", db.getDescription());
        assertFalse(Boolean.parseBoolean(db.getParameters().get(CatalogPropertiesUtil.IS_GENERIC)));

        String db2Location = warehouse + "/db2_location";
        tableEnv.executeSql(
                String.format(
                        "create database db2 location '%s' with dbproperties('k1'='v1')",
                        db2Location));
        db = hiveCatalog.getHiveDatabase("db2");
        assertEquals(db2Location, locationPath(db.getLocationUri()));
        assertEquals("v1", db.getParameters().get("k1"));
    }

    @Test
    public void testAlterDatabase() throws Exception {
        // alter properties
        tableEnv.executeSql("create database db1 with dbproperties('k1'='v1')");
        tableEnv.executeSql("alter database db1 set dbproperties ('k1'='v11','k2'='v2')");
        Database db = hiveCatalog.getHiveDatabase("db1");
        // there's an extra is_generic property
        assertEquals(3, db.getParametersSize());
        assertEquals("v11", db.getParameters().get("k1"));
        assertEquals("v2", db.getParameters().get("k2"));

        // alter owner
        tableEnv.executeSql("alter database db1 set owner user user1");
        db = hiveCatalog.getHiveDatabase("db1");
        assertEquals("user1", db.getOwnerName());
        assertEquals(PrincipalType.USER, db.getOwnerType());

        tableEnv.executeSql("alter database db1 set owner role role1");
        db = hiveCatalog.getHiveDatabase("db1");
        assertEquals("role1", db.getOwnerName());
        assertEquals(PrincipalType.ROLE, db.getOwnerType());

        // alter location
        if (hiveCatalog.getHiveVersion().compareTo("2.4.0") >= 0) {
            String newLocation = warehouse + "/db1_new_location";
            tableEnv.executeSql(String.format("alter database db1 set location '%s'", newLocation));
            db = hiveCatalog.getHiveDatabase("db1");
            assertEquals(newLocation, locationPath(db.getLocationUri()));
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
        assertEquals(TableType.EXTERNAL_TABLE.toString(), hiveTable.getTableType());
        assertEquals(1, hiveTable.getPartitionKeysSize());
        assertEquals(location, locationPath(hiveTable.getSd().getLocation()));
        assertEquals("v1", hiveTable.getParameters().get("k1"));
        assertFalse(hiveTable.getParameters().containsKey(SqlCreateHiveTable.TABLE_LOCATION_URI));

        tableEnv.executeSql("create table tbl2 (s struct<ts:timestamp,bin:binary>) stored as orc");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl2"));
        assertEquals(TableType.MANAGED_TABLE.toString(), hiveTable.getTableType());
        assertEquals(
                OrcSerde.class.getName(), hiveTable.getSd().getSerdeInfo().getSerializationLib());
        assertEquals(OrcInputFormat.class.getName(), hiveTable.getSd().getInputFormat());
        assertEquals(OrcOutputFormat.class.getName(), hiveTable.getSd().getOutputFormat());

        tableEnv.executeSql(
                "create table tbl3 (m map<timestamp,binary>) partitioned by (p1 bigint,p2 tinyint) "
                        + "row format serde 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe'");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl3"));
        assertEquals(2, hiveTable.getPartitionKeysSize());
        assertEquals(
                LazyBinarySerDe.class.getName(),
                hiveTable.getSd().getSerdeInfo().getSerializationLib());

        tableEnv.executeSql(
                "create table tbl4 (x int,y smallint) row format delimited fields terminated by '|' lines terminated by '\n'");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl4"));
        assertEquals(
                "|",
                hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.FIELD_DELIM));
        assertEquals(
                "|",
                hiveTable
                        .getSd()
                        .getSerdeInfo()
                        .getParameters()
                        .get(serdeConstants.SERIALIZATION_FORMAT));
        assertEquals(
                "\n",
                hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.LINE_DELIM));

        tableEnv.executeSql(
                "create table tbl5 (m map<bigint,string>) row format delimited collection items terminated by ';' "
                        + "map keys terminated by ':'");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl5"));
        assertEquals(
                ";",
                hiveTable
                        .getSd()
                        .getSerdeInfo()
                        .getParameters()
                        .get(serdeConstants.COLLECTION_DELIM));
        assertEquals(
                ":",
                hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.MAPKEY_DELIM));

        int createdTimeForTableExists = hiveTable.getCreateTime();
        tableEnv.executeSql("create table if not exists tbl5 (m map<bigint,string>)");
        hiveTable = hiveCatalog.getHiveTable(new ObjectPath("default", "tbl5"));
        assertEquals(createdTimeForTableExists, hiveTable.getCreateTime());
    }

    @Test
    public void testCreateTableWithConstraints() throws Exception {
        Assume.assumeTrue(HiveVersionTestUtil.HIVE_310_OR_LATER);
        tableEnv.executeSql(
                "create table tbl (x int,y int not null disable novalidate rely,z int not null disable novalidate norely,"
                        + "constraint pk_name primary key (x) rely)");
        CatalogTable catalogTable =
                (CatalogTable) hiveCatalog.getTable(new ObjectPath("default", "tbl"));
        TableSchema tableSchema = catalogTable.getSchema();
        assertTrue("PK not present", tableSchema.getPrimaryKey().isPresent());
        assertEquals("pk_name", tableSchema.getPrimaryKey().get().getName());
        assertFalse(
                "PK cannot be null",
                tableSchema.getFieldDataTypes()[0].getLogicalType().isNullable());
        assertFalse(
                "RELY NOT NULL should be reflected in schema",
                tableSchema.getFieldDataTypes()[1].getLogicalType().isNullable());
        assertTrue(
                "NORELY NOT NULL shouldn't be reflected in schema",
                tableSchema.getFieldDataTypes()[2].getLogicalType().isNullable());
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
        assertEquals("[+I[1], +I[2], +I[3]]", results.toString());
        tableEnv.executeSql("insert overwrite dest values (3),(4),(5)").await();
        results = queryResult(tableEnv.sqlQuery("select * from dest"));
        assertEquals("[+I[3], +I[4], +I[5]]", results.toString());

        // partitioned dest table
        tableEnv.executeSql("create table dest2 (x int) partitioned by (p1 int,p2 string)");
        tableEnv.executeSql("insert into dest2 partition (p1=0,p2='static') select x from src")
                .await();
        results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
        assertEquals("[+I[1, 0, static], +I[2, 0, static], +I[3, 0, static]]", results.toString());
        tableEnv.executeSql("insert into dest2 partition (p1=1,p2) select x,y from src").await();
        results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
        assertEquals(
                "[+I[1, 0, static], +I[1, 1, a], +I[2, 0, static], +I[2, 1, b], +I[3, 0, static], +I[3, 1, c]]",
                results.toString());
        tableEnv.executeSql("insert overwrite dest2 partition (p1,p2) select 1,x,y from src")
                .await();
        results = queryResult(tableEnv.sqlQuery("select * from dest2 order by x,p1,p2"));
        assertEquals(
                "[+I[1, 0, static], +I[1, 1, a], +I[1, 2, b], +I[1, 3, c], +I[2, 0, static], +I[2, 1, b], +I[3, 0, static], +I[3, 1, c]]",
                results.toString());
    }

    @Test
    public void testAlterTable() throws Exception {
        tableEnv.executeSql("create table tbl (x int) tblproperties('k1'='v1')");
        tableEnv.executeSql("alter table tbl rename to tbl1");

        ObjectPath tablePath = new ObjectPath("default", "tbl1");

        // change properties
        tableEnv.executeSql("alter table `default`.tbl1 set tblproperties ('k2'='v2')");
        Table hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertEquals("v1", hiveTable.getParameters().get("k1"));
        assertEquals("v2", hiveTable.getParameters().get("k2"));

        // change location
        String newLocation = warehouse + "/tbl1_new_location";
        tableEnv.executeSql(
                String.format("alter table default.tbl1 set location '%s'", newLocation));
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertEquals(newLocation, locationPath(hiveTable.getSd().getLocation()));

        // change file format
        tableEnv.executeSql("alter table tbl1 set fileformat orc");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertEquals(
                OrcSerde.class.getName(), hiveTable.getSd().getSerdeInfo().getSerializationLib());
        assertEquals(OrcInputFormat.class.getName(), hiveTable.getSd().getInputFormat());
        assertEquals(OrcOutputFormat.class.getName(), hiveTable.getSd().getOutputFormat());

        // change serde
        tableEnv.executeSql(
                String.format(
                        "alter table tbl1 set serde '%s' with serdeproperties('%s'='%s')",
                        LazyBinarySerDe.class.getName(), serdeConstants.FIELD_DELIM, "\u0001"));
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertEquals(
                LazyBinarySerDe.class.getName(),
                hiveTable.getSd().getSerdeInfo().getSerializationLib());
        assertEquals(
                "\u0001",
                hiveTable.getSd().getSerdeInfo().getParameters().get(serdeConstants.FIELD_DELIM));

        // replace columns
        tableEnv.executeSql(
                "alter table tbl1 replace columns (t tinyint,s smallint,i int,b bigint,f float,d double,num decimal,"
                        + "ts timestamp,dt date,str string,var varchar(10),ch char(123),bool boolean,bin binary)");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertEquals(14, hiveTable.getSd().getColsSize());
        assertEquals("varchar(10)", hiveTable.getSd().getCols().get(10).getType());
        assertEquals("char(123)", hiveTable.getSd().getCols().get(11).getType());

        tableEnv.executeSql(
                "alter table tbl1 replace columns (a array<array<int>>,s struct<f1:struct<f11:int,f12:binary>, f2:map<double,date>>,"
                        + "m map<char(5),map<timestamp,decimal(20,10)>>)");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertEquals("array<array<int>>", hiveTable.getSd().getCols().get(0).getType());
        assertEquals(
                "struct<f1:struct<f11:int,f12:binary>,f2:map<double,date>>",
                hiveTable.getSd().getCols().get(1).getType());
        assertEquals(
                "map<char(5),map<timestamp,decimal(20,10)>>",
                hiveTable.getSd().getCols().get(2).getType());

        // add columns
        tableEnv.executeSql("alter table tbl1 add columns (x int,y int)");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertEquals(5, hiveTable.getSd().getColsSize());

        // change column
        tableEnv.executeSql("alter table tbl1 change column x x1 string comment 'new x col'");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        assertEquals(5, hiveTable.getSd().getColsSize());
        FieldSchema newField = hiveTable.getSd().getCols().get(3);
        assertEquals("x1", newField.getName());
        assertEquals("string", newField.getType());

        tableEnv.executeSql("alter table tbl1 change column y y int first");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        newField = hiveTable.getSd().getCols().get(0);
        assertEquals("y", newField.getName());
        assertEquals("int", newField.getType());

        tableEnv.executeSql("alter table tbl1 change column x1 x2 timestamp after y");
        hiveTable = hiveCatalog.getHiveTable(tablePath);
        newField = hiveTable.getSd().getCols().get(1);
        assertEquals("x2", newField.getName());
        assertEquals("timestamp", newField.getType());

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
        assertEquals(2, hivePartition.getSd().getColsSize());
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec2);
        assertEquals(2, hivePartition.getSd().getColsSize());

        tableEnv.executeSql("alter table tbl2 add columns (ch char(5),vch varchar(9)) cascade");
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec1);
        assertEquals(4, hivePartition.getSd().getColsSize());
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec2);
        assertEquals(4, hivePartition.getSd().getColsSize());

        // change column cascade
        tableEnv.executeSql("alter table tbl2 change column ch ch char(10) cascade");
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec1);
        assertEquals("char(10)", hivePartition.getSd().getCols().get(2).getType());
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec2);
        assertEquals("char(10)", hivePartition.getSd().getCols().get(2).getType());

        tableEnv.executeSql("alter table tbl2 change column vch str string first cascade");
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec1);
        assertEquals("str", hivePartition.getSd().getCols().get(0).getName());
        hivePartition = hiveCatalog.getHivePartition(hiveTable, partitionSpec2);
        assertEquals("str", hivePartition.getSd().getCols().get(0).getName());
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
        assertEquals(location, locationPath(partition.getSd().getLocation()));

        // change file format
        tableEnv.executeSql(
                "alter table tbl partition (p1=2000,p2='2020-01-01') set fileformat rcfile");
        partition = hiveCatalog.getHivePartition(hiveTable, spec2);
        assertEquals(
                LazyBinaryColumnarSerDe.class.getName(),
                partition.getSd().getSerdeInfo().getSerializationLib());
        assertEquals(RCFileInputFormat.class.getName(), partition.getSd().getInputFormat());
        assertEquals(RCFileOutputFormat.class.getName(), partition.getSd().getOutputFormat());

        // change serde
        tableEnv.executeSql(
                String.format(
                        "alter table tbl partition (p1=1000,p2='2020-05-01') set serde '%s' with serdeproperties('%s'='%s')",
                        LazyBinarySerDe.class.getName(), serdeConstants.LINE_DELIM, "\n"));
        partition = hiveCatalog.getHivePartition(hiveTable, spec1);
        assertEquals(
                LazyBinarySerDe.class.getName(),
                partition.getSd().getSerdeInfo().getSerializationLib());
        assertEquals(
                "\n",
                partition.getSd().getSerdeInfo().getParameters().get(serdeConstants.LINE_DELIM));
    }

    @Test
    public void testView() throws Exception {
        tableEnv.executeSql("create table tbl (x int,y string)");

        // create
        tableEnv.executeSql(
                "create view v(vx) comment 'v comment' tblproperties ('k1'='v1') as select x from tbl");
        ObjectPath viewPath = new ObjectPath("default", "v");
        Table hiveView = hiveCatalog.getHiveTable(viewPath);
        assertEquals(TableType.VIRTUAL_VIEW.name(), hiveView.getTableType());
        assertEquals("vx", hiveView.getSd().getCols().get(0).getName());
        assertEquals("v1", hiveView.getParameters().get("k1"));

        // change properties
        tableEnv.executeSql("alter view v set tblproperties ('k1'='v11')");
        hiveView = hiveCatalog.getHiveTable(viewPath);
        assertEquals("v11", hiveView.getParameters().get("k1"));

        // change query
        tableEnv.executeSql("alter view v as select y from tbl");
        hiveView = hiveCatalog.getHiveTable(viewPath);
        assertEquals("y", hiveView.getSd().getCols().get(0).getName());

        // rename
        tableEnv.executeSql("alter view v rename to v1");
        viewPath = new ObjectPath("default", "v1");
        assertTrue(hiveCatalog.tableExists(viewPath));

        // drop
        tableEnv.executeSql("drop view v1");
        assertFalse(hiveCatalog.tableExists(viewPath));
    }

    @Test
    public void testFunction() throws Exception {
        // create function
        tableEnv.executeSql(
                String.format("create function my_abs as '%s'", GenericUDFAbs.class.getName()));
        List<Row> functions =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show functions").collect());
        assertTrue(functions.toString().contains("my_abs"));
        // call the function
        tableEnv.executeSql("create table src(x int)");
        tableEnv.executeSql("insert into src values (1),(-1)").await();
        assertEquals(
                "[+I[1], +I[1]]",
                queryResult(tableEnv.sqlQuery("select my_abs(x) from src")).toString());
        // drop the function
        tableEnv.executeSql("drop function my_abs");
        assertFalse(hiveCatalog.functionExists(new ObjectPath("default", "my_abs")));
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
        assertArrayEquals(new String[] {"temp_abs"}, functions);
        // call the function
        tableEnv.executeSql("create table src(x int)");
        tableEnv.executeSql("insert into src values (1),(-1)").await();
        assertEquals(
                "[+I[1], +I[1]]",
                queryResult(tableEnv.sqlQuery("select temp_abs(x) from src")).toString());
        // switch DB and the temp function can still be used
        tableEnv.executeSql("create database db1");
        tableEnv.useDatabase("db1");
        assertEquals(
                "[+I[1], +I[1]]",
                queryResult(tableEnv.sqlQuery("select temp_abs(x) from `default`.src")).toString());
        // drop the function
        tableEnv.executeSql("drop temporary function temp_abs");
        functions = tableEnv.listUserDefinedFunctions();
        assertEquals(0, functions.length);
        tableEnv.executeSql("drop temporary function if exists foo");
    }

    @Test
    public void testCatalog() {
        List<Row> catalogs =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show catalogs").collect());
        assertEquals(2, catalogs.size());
        tableEnv.executeSql("use catalog " + DEFAULT_BUILTIN_CATALOG);
        List<Row> databases =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show databases").collect());
        assertEquals(1, databases.size());
        assertEquals("+I[" + DEFAULT_BUILTIN_DATABASE + "]", databases.get(0).toString());
        String catalogName =
                tableEnv.executeSql("show current catalog").collect().next().toString();
        assertEquals("+I[" + DEFAULT_BUILTIN_CATALOG + "]", catalogName);
        String databaseName =
                tableEnv.executeSql("show current database").collect().next().toString();
        assertEquals("+I[" + DEFAULT_BUILTIN_DATABASE + "]", databaseName);
    }

    @Test
    public void testAddDropPartitions() throws Exception {
        tableEnv.executeSql(
                "create table tbl (x int,y binary) partitioned by (dt date,country string)");
        tableEnv.executeSql(
                "alter table tbl add partition (dt='2020-04-30',country='china') partition (dt='2020-04-30',country='us')");

        ObjectPath tablePath = new ObjectPath("default", "tbl");
        assertEquals(2, hiveCatalog.listPartitions(tablePath).size());

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
        assertEquals(partLocation, locationPath(hivePartition.getSd().getLocation()));

        tableEnv.executeSql(
                "alter table tbl drop partition (dt='2020-04-30',country='china'),partition (dt='2020-05-01',country='belgium')");
        assertEquals(1, hiveCatalog.listPartitions(tablePath).size());
    }

    @Test
    public void testShowPartitions() throws Exception {
        tableEnv.executeSql(
                "create table tbl (x int,y binary) partitioned by (dt date, country string)");
        tableEnv.executeSql(
                "alter table tbl add partition (dt='2020-04-30',country='china') partition (dt='2020-04-30',country='us')");

        ObjectPath tablePath = new ObjectPath("default", "tbl");
        assertEquals(2, hiveCatalog.listPartitions(tablePath).size());

        List<Row> partitions =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show partitions tbl").collect());
        assertEquals(2, partitions.size());
        assertTrue(partitions.toString().contains("dt=2020-04-30/country=china"));
        assertTrue(partitions.toString().contains("dt=2020-04-30/country=us"));
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("show partitions tbl partition (dt='2020-04-30')")
                                .collect());
        assertEquals(2, partitions.size());
        assertTrue(partitions.toString().contains("dt=2020-04-30/country=china"));
        assertTrue(partitions.toString().contains("dt=2020-04-30/country=us"));
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql("show partitions tbl partition (country='china')")
                                .collect());
        assertEquals(1, partitions.size());
        assertTrue(partitions.toString().contains("dt=2020-04-30/country=china"));
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-04-30',country='china')")
                                .collect());
        assertEquals(1, partitions.size());
        assertTrue(partitions.toString().contains("dt=2020-04-30/country=china"));
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-05-01',country='japan')")
                                .collect());
        assertEquals(0, partitions.size());
        try {
            CollectionUtil.iteratorToList(
                    tableEnv.executeSql(
                                    "show partitions tbl partition (de='2020-04-30',city='china')")
                            .collect());
        } catch (TableException e) {
            assertEquals(
                    String.format(
                            "Could not execute SHOW PARTITIONS %s.%s PARTITION (de=2020-04-30, city=china)",
                            hiveCatalog.getName(), tablePath),
                    e.getMessage());
        }

        tableEnv.executeSql(
                "alter table tbl drop partition (dt='2020-04-30',country='china'),partition (dt='2020-04-30',country='us')");
        assertEquals(0, hiveCatalog.listPartitions(tablePath).size());

        tableEnv.executeSql("drop table tbl");
        tableEnv.executeSql(
                "create table tbl (x int,y binary) partitioned by (dt timestamp, country string)");
        tableEnv.executeSql(
                "alter table tbl add partition (dt='2020-04-30 01:02:03',country='china') partition (dt='2020-04-30 04:05:06',country='us')");

        partitions =
                CollectionUtil.iteratorToList(tableEnv.executeSql("show partitions tbl").collect());
        assertEquals(2, partitions.size());
        assertTrue(partitions.toString().contains("dt=2020-04-30 01:02:03/country=china"));
        assertTrue(partitions.toString().contains("dt=2020-04-30 04:05:06/country=us"));
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-04-30 01:02:03')")
                                .collect());
        assertEquals(1, partitions.size());
        assertTrue(partitions.toString().contains("dt=2020-04-30 01:02:03/country=china"));
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-04-30 04:05:06')")
                                .collect());
        assertEquals(1, partitions.size());
        assertTrue(partitions.toString().contains("dt=2020-04-30 04:05:06/country=us"));
        partitions =
                CollectionUtil.iteratorToList(
                        tableEnv.executeSql(
                                        "show partitions tbl partition (dt='2020-04-30 01:02:03',country='china')")
                                .collect());
        assertEquals(1, partitions.size());
        assertTrue(partitions.toString().contains("dt=2020-04-30 01:02:03/country=china"));
    }

    private static String locationPath(String locationURI) throws URISyntaxException {
        return new URI(locationURI).getPath();
    }

    private static List<Row> queryResult(org.apache.flink.table.api.Table table) {
        return CollectionUtil.iteratorToList(table.execute().collect());
    }
}
