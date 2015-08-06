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

package org.apache.flink.hcatalog.java.test;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.hcatalog.java.HCatInputFormat;
import org.apache.flink.hcatalog.java.HCatOutputFormat;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HcatInputOutputFormatITest {

    private File dataDir;
    private String warehouseDir;
    private String inputFileName;
    private Driver driver;
    private String[] input;
    private HiveConf hiveConf;

    @Before
    public void setup() throws Exception {
        dataDir = new File(System.getProperty("java.io.tmpdir") + File.separator +
                HcatInputOutputFormatITest.class.getCanonicalName() + "-" +
                System.currentTimeMillis());
        hiveConf = new HiveConf();
        warehouseDir = makePathASafeFileName(dataDir + File.separator + "warehouse");

        hiveConf.set(HiveConf.ConfVars.PREEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.POSTEXECHOOKS.varname, "");
        hiveConf.set(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, warehouseDir);
        driver = new Driver(hiveConf);
        SessionState.start(new CliSessionState(hiveConf));

        if(!(new File(warehouseDir).mkdirs())) {
            throw new RuntimeException("Could not create " + warehouseDir);
        }


    }

    @After
    public void teardown() throws IOException {
        if(dataDir != null) {
            FileUtils.deleteDirectory(dataDir);
        }
    }

    @Test
    public void testReadTextFile() throws Exception {
        //create input
        inputFileName = makePathASafeFileName(dataDir + File.separator + "input1.data");
        int numRows = 3;
        input = new String[numRows];
        for (int i = 0; i < numRows; i++) {
            String col1 = "a" + i;
            String col2 = "b" + i;
            input[i] = i + "\t" + col1 + "\t" + col2;
        }
        createTestDataFile(inputFileName, input);

        String createTable = "CREATE TABLE test_table1(a0 int, a1 String, a2 String)row format " +
                "delimited fields terminated by '\t' STORED AS TEXTFILE";
        driver.run("drop table test_table1");
        int retCode1 = driver.run(createTable).getResponseCode();
        assertTrue(retCode1 == 0);

        String loadTable = "load data inpath '" + inputFileName + "' into table test_table1";
        int retCode2 = driver.run(loadTable).getResponseCode();
        assertTrue(retCode2 == 0);

        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        HCatInputFormat<Tuple3> ipf = (HCatInputFormat<Tuple3>)
        new HCatInputFormat<Tuple3>(null, "test_table1", hiveConf).asFlinkTuples();
        DataSet<Tuple3> d = env.createInput(ipf);
        List<Tuple3> l = d.collect();
        Integer i = 0;
        for(Tuple3 t: l)
        {
            assertEquals(t.f0, i);
            assertEquals(t.f1, "a"+ i);
            assertEquals(t.f2, "b" + i);
            i++;
        }

    }

    @Test
    public void testReadComplexType() throws Exception {
        //create input
        inputFileName = makePathASafeFileName(dataDir + File.separator + "input2.data");
        int numRows = 2;
        input = new String[numRows];
        input[0] = "a1/b1/c1\t1:v11/2:v12\td/1";
        input[1] = "a2/b2/c2\t1:v21/2:v22\td/2";
        createTestDataFile(inputFileName, input);

        String createTable = "CREATE TABLE test_table2(c1 array<string>,\n" +
                "c2 map<int,string>,\n" +
                "c3 struct<name:string,score:int>)row format delimited " +
                "fields terminated by '\t' " +
                "COLLECTION ITEMS TERMINATED BY '/' " +
                "MAP KEYS TERMINATED BY ':' " +
                "STORED AS TEXTFILE";
        driver.run("drop table test_table2");
        int retCode1 = driver.run(createTable).getResponseCode();
        assertTrue(retCode1 == 0);

        String loadTable = "load data inpath '" + inputFileName + "' into table test_table2";
        int retCode2 = driver.run(loadTable).getResponseCode();
        assertTrue(retCode2 == 0);

        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();
        HCatInputFormat<Tuple3> ipf = (HCatInputFormat<Tuple3>)
                new HCatInputFormat<Tuple3>(null, "test_table2", hiveConf).asFlinkTuples();
        DataSet<Tuple3> d = env.createInput(ipf);
        List<Tuple3> l = d.collect();

        //just check the first row
        Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>> t= l.get(0);
        assertEquals(t.f0.get(0), "a1");
        assertEquals(t.f0.get(1), "b1");
        assertEquals(t.f0.get(2), "c1");
        assertEquals(t.f1.get(1), "v11");
        assertEquals(t.f1.get(2), "v12");
        assertEquals(t.f2.get(0), "d");
        assertEquals(t.f2.get(1), 1);
    }

    @Test
    public void testWriteComplexType() throws Exception {

        String createTable = "CREATE TABLE test_table3(c1 array<string>,\n" +
                "c2 map<int,string>,\n" +
                "c3 struct<name:string,score:int>)row format delimited " +
                "fields terminated by '\t' " +
                "COLLECTION ITEMS TERMINATED BY '/' " +
                "MAP KEYS TERMINATED BY ':' " +
                "STORED AS TEXTFILE";
        driver.run("drop table test_table3");
        int retCode1 = driver.run(createTable).getResponseCode();
        assertTrue(retCode1 == 0);

        HCatOutputFormat<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> opf
                = new HCatOutputFormat<>(null, "test_table3",null, hiveConf);


        List<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> l =
                new ArrayList<>(2);
        Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>> t1 = new Tuple3<>();
        t1.f0 = new ArrayList<>();
        t1.f0.add("a1");
        t1.f0.add("b1");
        t1.f0.add("c1");
        t1.f1 = new HashMap<>();
        t1.f1.put(1, "v11");
        t1.f1.put(2, "v12");
        t1.f2 = new ArrayList<>();
        t1.f2.add("d");
        t1.f2.add(1);
        Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>> t2 = new Tuple3<>();
        t2.f0 = new ArrayList<>();
        t2.f0.add("a2");
        t2.f0.add("b2");
        t2.f0.add("c2");
        t2.f1 = new HashMap<>();
        t2.f1.put(1, "v21");
        t2.f1.put(2, "v22");
        t2.f2 = new ArrayList<>();
        t2.f2.add("d");
        t2.f2.add(2);
        l.add(t1);
        l.add(t2);

        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        DataSource<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> d =
                env.fromCollection(l);
        d.output(opf);
        env.execute();

        //verify the result has been writen to the table, we don't want to depend on flink test
        String outputFileName = makePathASafeFileName(warehouseDir + File.separator +
                "test_table3/part-m-00001");
        BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
        try {
            String line = reader.readLine();
            assertEquals("1st row", line, "a1/b1/c1\t1:v11/2:v12\td/1");
            line = reader.readLine();
            assertEquals("2nd row", line, "a2/b2/c2\t1:v21/2:v22\td/2");
            reader.close();
        } catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteComplexTypeParition() throws Exception {

        String createTable = "CREATE TABLE test_table4(" +
                "c1 array<string>,\n" +
                "c2 map<int,string>,\n" +
                "c3 struct<name:string,score:int>)\n" +
                "partitioned by (c0 string)\n" +
                "row format delimited " +
                "fields terminated by '\t' " +
                "COLLECTION ITEMS TERMINATED BY '/' " +
                "MAP KEYS TERMINATED BY ':' " +
                "STORED AS TEXTFILE";
        driver.run("drop table test_table4");
        int retCode1 = driver.run(createTable).getResponseCode();
        assertTrue(retCode1 == 0);

        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("c0", "part0");
        HCatOutputFormat<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> opf
                = new HCatOutputFormat<>(null, "test_table4",partitionValues, hiveConf);


        List<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> l =
                new ArrayList<>(2);
        Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>> t1 = new Tuple3<>();
        t1.f0 = new ArrayList<>();
        t1.f0.add("a1");
        t1.f0.add("b1");
        t1.f0.add("c1");
        t1.f1 = new HashMap<>();
        t1.f1.put(1, "v11");
        t1.f1.put(2, "v12");
        t1.f2 = new ArrayList<>();
        t1.f2.add("d");
        t1.f2.add(1);
        Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>> t2 = new Tuple3<>();
        t2.f0 = new ArrayList<>();
        t2.f0.add("a2");
        t2.f0.add("b2");
        t2.f0.add("c2");
        t2.f1 = new HashMap<>();
        t2.f1.put(1, "v21");
        t2.f1.put(2, "v22");
        t2.f2 = new ArrayList<>();
        t2.f2.add("d");
        t2.f2.add(2);
        l.add(t1);
        l.add(t2);

        ExecutionEnvironment env = ExecutionEnvironment.createCollectionsEnvironment();

        DataSource<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> d =
                env.fromCollection(l);
        d.output(opf);
        env.execute();

        //verify the result has been written to the table
        String outputFileName = makePathASafeFileName(warehouseDir + File.separator +
                "test_table4/c0=part0/part-m-00001");
        BufferedReader reader = new BufferedReader(new FileReader(outputFileName));
        try {
            String line = reader.readLine();
            assertEquals("1st row", line, "a1/b1/c1\t1:v11/2:v12\td/1");
            line = reader.readLine();
            assertEquals("2nd row", line, "a2/b2/c2\t1:v21/2:v22\td/2");
            reader.close();
        } catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testTypeInfocheck() throws Exception {

        String createTable = "CREATE TABLE test_table4(" +
                "c1 array<string>,\n" +
                "c2 map<int,string>,\n" +
                "c3 struct<name:string,score:int>)\n" +
                "partitioned by (c0 string)\n" +
                "row format delimited " +
                "fields terminated by '\t' " +
                "COLLECTION ITEMS TERMINATED BY '/' " +
                "MAP KEYS TERMINATED BY ':' " +
                "STORED AS TEXTFILE";
        driver.run("drop table test_table4");
        int retCode1 = driver.run(createTable).getResponseCode();
        assertTrue(retCode1 == 0);

        Map<String, String> partitionValues = new HashMap<>();
        partitionValues.put("c0", "part0");
        //should not be able to create a HCatOutputFormat of
        // Tuple3<ArrayList<String>, HashMap<Integer, String>, String>
        //when the hcat schema requires
        //Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList>
        try {
            @SuppressWarnings("unchecked")
            HCatOutputFormat<Tuple3<ArrayList<String>, HashMap<Integer, String>, String>> opf
                    = new HCatOutputFormat<>(null, "test_table4", partitionValues, hiveConf,
                    new TupleTypeInfo(new GenericTypeInfo(List.class),
                            new GenericTypeInfo(Map.class),
                            BasicTypeInfo.STRING_TYPE_INFO));
            fail("expecting exception");
        } catch (IOException e) {
            assertEquals(e.getMessage(), "tuple has different types from the table's columns");
        }

        //this should succeed!
        try{
            @SuppressWarnings("unchecked")
            HCatOutputFormat<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>>
                    opf
                    = new HCatOutputFormat<>(null, "test_table4", partitionValues, hiveConf,
                    new TupleTypeInfo(new GenericTypeInfo(List.class),
                            new GenericTypeInfo(Map.class),
                            new GenericTypeInfo(List.class)));
        }catch (IOException e) {
            fail("should be able to construct HCatOutputFormat for correct types");
        }


    }
    public static void createTestDataFile(String filename, String[] lines) throws IOException {
        FileWriter writer = null;
        try {
            File file = new File(filename);
            file.deleteOnExit();
            writer = new FileWriter(file);
            for (String line : lines) {
                writer.write(line + "\n");
            }
        } finally {
            if (writer != null) {
                writer.close();
            }
        }
    }
    public static String makePathASafeFileName(String filePath) {
        return new File(filePath).getPath().replaceAll("\\\\", "/");
    }
}
