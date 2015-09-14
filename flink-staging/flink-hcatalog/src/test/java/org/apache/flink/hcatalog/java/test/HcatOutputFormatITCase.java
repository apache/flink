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
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.hcatalog.java.HCatOutputFormat;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HcatOutputFormatITCase {
	private File dataDir;
	private String warehouseDir;
	private Driver driver;
	private HiveConf hiveConf;

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Before
	public void setup() throws Exception {
		dataDir = new File(System.getProperty("java.io.tmpdir") + File.separator +
				HcatOutputFormatITCase.class.getCanonicalName() + "-" +
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
	public void testWriteComplexType() throws Exception {
		String createTable = "CREATE TABLE test_table0(c1 array<string>,\n" +
				"c2 map<int,string>,\n" +
				"c3 struct<name:string,score:int>)row format delimited " +
				"fields terminated by '\t' " +
				"COLLECTION ITEMS TERMINATED BY '/' " +
				"MAP KEYS TERMINATED BY ':' " +
				"STORED AS TEXTFILE";
		driver.run("drop table test_table0");
		int retCode1 = driver.run(createTable).getResponseCode();
		assertTrue("test_table0 created", retCode1 == 0);
		HCatOutputFormat<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> opf
				= new HCatOutputFormat<>(null, "test_table0",null, hiveConf);
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
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		DataSource<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> d =
				env.fromCollection(l);
		d.output(opf);
		env.execute();
		//verify the result has been written to the table
		String outputFileName = makePathASafeFileName("file://" + warehouseDir + File.separator +
				"test_table0/part-m-00001");
		String expected = "a1/b1/c1\t1:v11/2:v12\td/1\n" + "a2/b2/c2\t1:v21/2:v22\td/2";
		TestBaseUtils.compareResultsByLinesInMemory(expected, outputFileName);
	}

	@Test
	public void testWriteComplexTypePartition() throws Exception {
		String createTable = "CREATE TABLE test_table1(" +
				"c1 array<string>,\n" +
				"c2 map<int,string>,\n" +
				"c3 struct<name:string,score:int>)\n" +
				"partitioned by (c0 string)\n" +
				"row format delimited " +
				"fields terminated by '\t' " +
				"COLLECTION ITEMS TERMINATED BY '/' " +
				"MAP KEYS TERMINATED BY ':' " +
				"STORED AS TEXTFILE";
		driver.run("drop table test_table1");
		int retCode1 = driver.run(createTable).getResponseCode();
		assertTrue("test_table1 created", retCode1 == 0);

		Map<String, String> partitionValues = new HashMap<>();
		partitionValues.put("c0", "part0");
		HCatOutputFormat<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> opf
				= new HCatOutputFormat<>(null, "test_table1",partitionValues, hiveConf);

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
		ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
		DataSource<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>> d =
				env.fromCollection(l);
		d.output(opf);
		env.execute();
		//verify the result has been written to the table
		String outputFileName = makePathASafeFileName("file://" + warehouseDir + File.separator +
				"test_table1/c0=part0/part-m-00001");
		String expected = "a1/b1/c1\t1:v11/2:v12\td/1\n" + "a2/b2/c2\t1:v21/2:v22\td/2";
		TestBaseUtils.compareResultsByLinesInMemory(expected, outputFileName);
	}

	@Test
	public void testTypeInfoCheck() throws Exception {
		String createTable = "CREATE TABLE test_table2(" +
				"c1 array<string>,\n" +
				"c2 map<int,string>,\n" +
				"c3 struct<name:string,score:int>)\n" +
				"partitioned by (c0 string)\n" +
				"row format delimited " +
				"fields terminated by '\t' " +
				"COLLECTION ITEMS TERMINATED BY '/' " +
				"MAP KEYS TERMINATED BY ':' " +
				"STORED AS TEXTFILE";
		driver.run("drop table test_table2");
		int retCode1 = driver.run(createTable).getResponseCode();
		assertTrue("test_table2 created", retCode1 == 0);

		Map<String, String> partitionValues = new HashMap<>();
		partitionValues.put("c0", "part0");
		//should not be able to create a HCatOutputFormat of
		// Tuple3<ArrayList<String>, HashMap<Integer, String>, String>
		//when the hcat schema requires
		//Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList>
		exception.expect(IOException.class);
		@SuppressWarnings("unchecked")
		HCatOutputFormat<Tuple3<ArrayList<String>, HashMap<Integer, String>, String>> opf
				= new HCatOutputFormat<>(null, "test_table2", partitionValues, hiveConf,
				new TupleTypeInfo(new GenericTypeInfo(List.class),
						new GenericTypeInfo(Map.class),
						BasicTypeInfo.STRING_TYPE_INFO));
		//this should succeed!
		try {
			@SuppressWarnings("unchecked")
			HCatOutputFormat<Tuple3<ArrayList<String>, HashMap<Integer, String>, ArrayList<Object>>>
					opf1 = new HCatOutputFormat<>(null, "test_table2", partitionValues, hiveConf,
												new TupleTypeInfo(new GenericTypeInfo(List.class),
														new GenericTypeInfo(Map.class),
														new GenericTypeInfo(List.class)));
		} catch (IOException e) {
			fail("should be able to construct HCatOutputFormat for correct types");
		}
	}

	public static String makePathASafeFileName(String filePath) {
		return new File(filePath).getPath().replaceAll("\\\\", "/");
	}
}
