/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.avro;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.io.AvroOutputFormat;
import eu.stratosphere.api.java.record.io.avro.example.User;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.test.util.JavaProgramTestBase;
import junit.framework.Assert;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.File;
import java.util.ArrayList;
import java.util.List;


public class AvroOutputFormatTest extends JavaProgramTestBase {

	public static String outputPath1;

	public static String outputPath2;

	public static String inputPath;

	public static String userData = "alice|1|blue\n" +
		"bob|2|red\n" +
		"john|3|yellow\n" +
		"walt|4|black\n";

	@Override
	protected void preSubmit() throws Exception {
		inputPath = createTempFile("user", userData);
		outputPath1 = getTempDirPath("avro_output1");
		outputPath2 = getTempDirPath("avro_output2");
	}


	@Override
	protected void testProgram() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple3<String, Integer, String>> input = env.readCsvFile(inputPath)
			.fieldDelimiter('|')
			.types(String.class, Integer.class, String.class);

		//output the data with AvroOutputFormat for specific user type
		DataSet<User> specificUser = input.map(new ConvertToUser());
		specificUser.write(new AvroOutputFormat<User>(User.class), outputPath1);

		//output the data with AvroOutputFormat for reflect user type
		DataSet<ReflectiveUser> reflectiveUser = specificUser.map(new ConvertToReflective());
		reflectiveUser.write(new AvroOutputFormat<ReflectiveUser>(ReflectiveUser.class), outputPath2);

		env.execute();
	}

	@Override
	protected void postSubmit() throws Exception {
		//compare result for specific user type
		File [] output1;
		File file1 = asFile(outputPath1);
		if (file1.isDirectory()) {
			output1 = file1.listFiles();
		} else {
			output1 = new File[] {file1};
		}
		List<String> result1 = new ArrayList<String>();
		DatumReader<User> userDatumReader1 = new SpecificDatumReader<User>(User.class);
		for (File avroOutput : output1) {
			DataFileReader<User> dataFileReader1 = new DataFileReader<User>(avroOutput, userDatumReader1);
			while (dataFileReader1.hasNext()) {
				User user = dataFileReader1.next();
				result1.add(user.getName() + "|" + user.getFavoriteNumber() + "|" + user.getFavoriteColor());
			}
		}
		for (String expectedResult : userData.split("\n")) {
			Assert.assertTrue("expected user " + expectedResult + " not found.", result1.contains(expectedResult));
		}

		//compare result for reflect user type
		File [] output2;
		File file2 = asFile(outputPath2);
		if (file2.isDirectory()) {
			output2 = file2.listFiles();
		} else {
			output2 = new File[] {file2};
		}
		List<String> result2 = new ArrayList<String>();
		DatumReader<ReflectiveUser> userDatumReader2 = new ReflectDatumReader<ReflectiveUser>(ReflectiveUser.class);
		for (File avroOutput : output2) {
			DataFileReader<ReflectiveUser> dataFileReader2 = new DataFileReader<ReflectiveUser>(avroOutput, userDatumReader2);
			while (dataFileReader2.hasNext()) {
				ReflectiveUser user = dataFileReader2.next();
				result2.add(user.getName() + "|" + user.getFavoriteNumber() + "|" + user.getFavoriteColor());
			}
		}
		for (String expectedResult : userData.split("\n")) {
			Assert.assertTrue("expected user " + expectedResult + " not found.", result2.contains(expectedResult));
		}


	}


	public final static class ConvertToUser extends MapFunction<Tuple3<String, Integer, String>, User> {

		@Override
		public User map(Tuple3<String, Integer, String> value) throws Exception {
			return new User(value.f0, value.f1, value.f2);
		}
	}

	public final static class ConvertToReflective extends MapFunction<User, ReflectiveUser> {

		@Override
		public ReflectiveUser map(User value) throws Exception {
			return new ReflectiveUser(value.getName().toString(), value.getFavoriteNumber(), value.getFavoriteColor().toString());
		}
	}

	
	public static class ReflectiveUser {
		private String name;
		private int favoriteNumber;
		private String favoriteColor;

		public ReflectiveUser() {}

		public ReflectiveUser(String name, int favoriteNumber, String favoriteColor) {
			this.name = name;
			this.favoriteNumber = favoriteNumber;
			this.favoriteColor = favoriteColor;
		}
		
		public String getName() {
			return this.name;
		}
		public String getFavoriteColor() {
			return this.favoriteColor;
		}
		public int getFavoriteNumber() {
			return this.favoriteNumber;
		}
	}
}
