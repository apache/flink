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
import org.apache.flink.api.java.io.AvroInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class AvroPojoTest extends MultipleProgramsTestBase {
	public AvroPojoTest(ExecutionMode mode) {
		super(mode);
	}

	private File inFile;
	private String resultPath;
	private String expected;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
		inFile = tempFolder.newFile();
		AvroRecordInputFormatTest.writeTestFile(inFile);
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expected, resultPath);
	}

	@Test
	public void testSimpleAvroRead() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Path in = new Path(inFile.getAbsoluteFile().toURI());

		AvroInputFormat<User> users = new AvroInputFormat<User>(in, User.class);
		DataSet<User> usersDS = env.createInput(users)
				// null map type because the order changes in different JVMs (hard to test)
				.map(new MapFunction<User, User>() {
			@Override
			public User map(User value) throws Exception {
				value.setTypeMap(null);
				return value;
			}
		});

		usersDS.writeAsText(resultPath);

		env.execute("Simple Avro read job");


		expected = "{\"name\": \"Alyssa\", \"favorite_number\": 256, \"favorite_color\": null, \"type_long_test\": null, \"type_double_test\": 123.45, \"type_null_test\": null, \"type_bool_test\": true, \"type_array_string\": [\"ELEMENT 1\", \"ELEMENT 2\"], \"type_array_boolean\": [true, false], \"type_nullable_array\": null, \"type_enum\": \"GREEN\", \"type_map\": null, \"type_fixed\": null, \"type_union\": null}\n" +
				"{\"name\": \"Charlie\", \"favorite_number\": null, \"favorite_color\": \"blue\", \"type_long_test\": 1337, \"type_double_test\": 1.337, \"type_null_test\": null, \"type_bool_test\": false, \"type_array_string\": [], \"type_array_boolean\": [], \"type_nullable_array\": null, \"type_enum\": \"RED\", \"type_map\": null, \"type_fixed\": null, \"type_union\": null}\n";
	}

	@Test
	public void testKeySelection() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Path in = new Path(inFile.getAbsoluteFile().toURI());

		AvroInputFormat<User> users = new AvroInputFormat<User>(in, User.class);
		DataSet<User> usersDS = env.createInput(users);

		DataSet<Tuple2<String, Integer>> res = usersDS.groupBy("name").reduceGroup(new GroupReduceFunction<User, Tuple2<String, Integer>>() {
			@Override
			public void reduce(Iterable<User> values, Collector<Tuple2<String, Integer>> out) throws Exception {
				for(User u : values) {
					out.collect(new Tuple2<String, Integer>(u.getName().toString(), 1));
				}
			}
		});

		res.writeAsText(resultPath);
		env.execute("Avro Key selection");


		expected = "(Alyssa,1)\n(Charlie,1)\n";
	}

	/**
	 * Test some know fields for grouping on
	 */
	@Test
	public void testAllFields() throws Exception {
		for(String fieldName : Arrays.asList("name", "type_enum", "type_double_test")) {
			testField(fieldName);
		}
	}

	private void testField(final String fieldName) throws Exception {
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
			expected = "Alyssa\nCharlie";
		} else if(fieldName.equals("type_enum")) {
			expected = "GREEN\nRED\n";
		} else if(fieldName.equals("type_double_test")) {
			expected = "123.45\n1.337\n";
		} else {
			Assert.fail("Unknown field");
		}

		after();
	}
}
