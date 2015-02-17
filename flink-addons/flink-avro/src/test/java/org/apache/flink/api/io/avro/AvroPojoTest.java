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
package org.apache.flink.api.io.avro;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.io.avro.generated.User;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.GenericAvroTypeInfo;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;

@RunWith(Parameterized.class)
public class AvroPojoTest extends JavaProgramTestBase {

	public AvroPojoTest(Configuration config) {
		super(config);
	}

	private File inFile;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
		inFile = tempFolder.newFile();
		AvroRecordInputFormatTest.writeTestFile(inFile);
	}


	private String testField(final String fieldName) throws Exception {
		before();

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Path in = new Path(inFile.getAbsoluteFile().toURI());

		AvroInputFormat<User> users = new AvroInputFormat<User>(in, User.class);
		DataSet<User> usersDS = env.createInput(users);

		DataSet<Object> res = usersDS.groupBy(fieldName).reduceGroup(new GroupReduceFunction<User, Object>() {
			@Override
			public void reduce(Iterable<User> values, Collector<Object> out) throws Exception {
				for(User u : values) {
					out.collect(u.get(fieldName));
				}
			}
		});
		res.writeAsText(resultPath);
		env.execute("Simple Avro read job");
		if(fieldName.equals("name")) {
			return "Alyssa\nCharlie";
		} else if(fieldName.equals("type_enum")) {
			return "GREEN\nRED\n";
		} else if(fieldName.equals("type_double_test")) {
			return "123.45\n1.337\n";
		} else {
			Assert.fail("Unknown field");
		}

		postSubmit();
		return "";
	}

	private static int NUM_PROGRAMS = 5;

	private int curProgId = config.getInteger("ProgramId", -1);
	private String resultPath;
	private String expectedResult;

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		expectedResult = runProgram(curProgId);
	}

	private String runProgram(int curProgId) throws Exception {
		switch(curProgId) {
			case 1:
				for (String fieldName : Arrays.asList("name", "type_enum", "type_double_test")) {
					return testField(fieldName);
				}
				break;
			case 2:
				ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				Path in = new Path(inFile.getAbsoluteFile().toURI());

				AvroInputFormat<User> users = new AvroInputFormat<User>(in, User.class);
				DataSet<User> usersDS = env.createInput(users);

				DataSet<Tuple2<String, Integer>> res = usersDS.groupBy("name").reduceGroup(new GroupReduceFunction<User, Tuple2<String, Integer>>() {
					@Override
					public void reduce(Iterable<User> values, Collector<Tuple2<String, Integer>> out) throws Exception {
						for (User u : values) {
							out.collect(new Tuple2<String, Integer>(u.getName().toString(), 1));
						}
					}
				});

				res.writeAsText(resultPath);
				env.execute("Avro Key selection");


				return "(Alyssa,1)\n(Charlie,1)\n";
			case 3:
				env = ExecutionEnvironment.getExecutionEnvironment();
				in = new Path(inFile.getAbsoluteFile().toURI());

				AvroInputFormat<User> users1 = new AvroInputFormat<User>(in, User.class);
				Assert.assertTrue(users1.getProducedType() instanceof PojoTypeInfo);
				DataSet<User> usersDS1 = env.createInput(users1)
						// null map type because the order changes in different JVMs (hard to test)
						.map(new MapFunction<User, User>() {
							@Override
							public User map(User value) throws Exception {
								value.setTypeMap(null);
								return value;
							}
						});

				usersDS1.writeAsText(resultPath);

				env.execute("Simple Avro read job");


				return "{\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": null, \"type_long_test\": null, \"type_double_test\": 123.45, \"type_null_test\": null, \"type_bool_test\": true, \"type_array_string\": [\"ELEMENT 1\", \"ELEMENT 2\"], \"type_array_boolean\": [true, false], \"type_nullable_array\": null, \"type_enum\": \"GREEN\", \"type_map\": null, \"type_fixed\": null, \"type_union\": null}\n" +
						"{\"name\": \"Charlie\", \"favorite_number\": null, \"favorite_color\": \"blue\", \"type_long_test\": 1337, \"type_double_test\": 1.337, \"type_null_test\": null, \"type_bool_test\": false, \"type_array_string\": [], \"type_array_boolean\": [], \"type_nullable_array\": null, \"type_enum\": \"RED\", \"type_map\": null, \"type_fixed\": null, \"type_union\": null}\n";
			case 4:
				/**
				 * Test GenericTypeInfo with Avro serialization.
				 */
				env = ExecutionEnvironment.getExecutionEnvironment();
				GenericTypeInfo.USE_AVRO_SERIALIZER = true;
				in = new Path(inFile.getAbsoluteFile().toURI());

				AvroInputFormat<User> users2 = new AvroInputFormat<User>(in, User.class);
				DataSet<User> usersDS2 = env.createInput(users2, new GenericTypeInfo<User>(User.class));

				DataSet<Tuple2<String, Integer>> res2 = usersDS2.groupBy(new KeySelector<User, String>() {
					@Override
					public String getKey(User value) throws Exception {
						return String.valueOf(value.getName());
					}
				}).reduceGroup(new GroupReduceFunction<User, Tuple2<String, Integer>>() {
					@Override
					public void reduce(Iterable<User> values, Collector<Tuple2<String, Integer>> out) throws Exception {
						for (User u : values) {
							out.collect(new Tuple2<String, Integer>(u.getName().toString(), 1));
						}
					}
				});

				res2.writeAsText(resultPath);
				env.execute("Avro Key selection");


				return "(Alyssa,1)\n(Charlie,1)\n";
			case 5:
				/**
				 * Test GenericAvroTypeInfo with Avro serialization.
				 */
				env = ExecutionEnvironment.getExecutionEnvironment();
				in = new Path(inFile.getAbsoluteFile().toURI());

				AvroInputFormat<User> users3 = new AvroInputFormat<User>(in, User.class);
				DataSet<User> usersDS3 = env.createInput(users3, new GenericAvroTypeInfo<User>(User.class));

				DataSet<Tuple2<String, Integer>> res3 = usersDS3.groupBy(new KeySelector<User, String>() {
					@Override
					public String getKey(User value) throws Exception {
						return String.valueOf(value.getName());
					}
				}).reduceGroup(new GroupReduceFunction<User, Tuple2<String, Integer>>() {
					@Override
					public void reduce(Iterable<User> values, Collector<Tuple2<String, Integer>> out) throws Exception {
						for (User u : values) {
							out.collect(new Tuple2<String, Integer>(u.getName().toString(), 1));
						}
					}
				});

				res3.writeAsText(resultPath);
				env.execute("Avro Key selection");


				return "(Alyssa,1)\n(Charlie,1)\n";
			default:
				throw new RuntimeException("Unknown test");
		}
		return "";
	}

	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Parameterized.Parameters
	public static Collection<Object[]> getConfigurations() throws IOException {

		LinkedList<Configuration> tConfigs = new LinkedList<Configuration>();

		for(int i=1; i <= NUM_PROGRAMS; i++) {
			Configuration config = new Configuration();
			config.setInteger("ProgramId", i);
			tConfigs.add(config);
		}

		return toParameterList(tConfigs);
	}
}
