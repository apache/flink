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

package org.apache.flink.formats.avro.testjar;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.AvroInputFormat;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * This file defines the classes for the AvroExternalJarProgramITCase.
 */
public class AvroExternalJarProgram  {

	private static final class Color {

		private String name;
		private double saturation;

		public Color() {
			name = "";
			saturation = 1.0;
		}

		public Color(String name, double saturation) {
			this.name = name;
			this.saturation = saturation;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public double getSaturation() {
			return saturation;
		}

		public void setSaturation(double saturation) {
			this.saturation = saturation;
		}

		@Override
		public String toString() {
			return name + '(' + saturation + ')';
		}
	}

	private static final class MyUser {

		private String name;
		private List<Color> colors;

		public MyUser() {
			name = "unknown";
			colors = new ArrayList<Color>();
		}

		public MyUser(String name, List<Color> colors) {
			this.name = name;
			this.colors = colors;
		}

		public String getName() {
			return name;
		}

		public List<Color> getColors() {
			return colors;
		}

		public void setName(String name) {
			this.name = name;
		}

		public void setColors(List<Color> colors) {
			this.colors = colors;
		}

		@Override
		public String toString() {
			return name + " : " + colors;
		}
	}

	// --------------------------------------------------------------------------------------------

	// --------------------------------------------------------------------------------------------

	private static final class NameExtractor extends RichMapFunction<MyUser, Tuple2<String, MyUser>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, MyUser> map(MyUser u) {
			String namePrefix = u.getName().substring(0, 1);
			return new Tuple2<String, MyUser>(namePrefix, u);
		}
	}

	private static final class NameGrouper extends RichReduceFunction<Tuple2<String, MyUser>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, MyUser> reduce(Tuple2<String, MyUser> val1, Tuple2<String, MyUser> val2) {
			return val1;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Test Data
	// --------------------------------------------------------------------------------------------

	private static final class Generator {

		private final Random rnd = new Random(2389756789345689276L);

		public MyUser nextUser() {
			return randomUser();
		}

		private MyUser randomUser() {

			int numColors = rnd.nextInt(5);
			ArrayList<Color> colors = new ArrayList<Color>(numColors);
			for (int i = 0; i < numColors; i++) {
				colors.add(new Color(randomString(), rnd.nextDouble()));
			}

			return new MyUser(randomString(), colors);
		}

		private String randomString() {
			char[] c = new char[this.rnd.nextInt(20) + 5];

			for (int i = 0; i < c.length; i++) {
				c[i] = (char) (this.rnd.nextInt(150) + 40);
			}

			return new String(c);
		}
	}

	public static void writeTestData(File testFile, int numRecords) throws IOException {

		DatumWriter<MyUser> userDatumWriter = new ReflectDatumWriter<MyUser>(MyUser.class);
		DataFileWriter<MyUser> dataFileWriter = new DataFileWriter<MyUser>(userDatumWriter);

		dataFileWriter.create(ReflectData.get().getSchema(MyUser.class), testFile);

		Generator generator = new Generator();

		for (int i = 0; i < numRecords; i++) {
			MyUser user = generator.nextUser();
			dataFileWriter.append(user);
		}

		dataFileWriter.close();
	}

//	public static void main(String[] args) throws Exception {
//		String testDataFile = new File("src/test/resources/testdata.avro").getAbsolutePath();
//		writeTestData(new File(testDataFile), 50);
//	}

	public static void main(String[] args) throws Exception {
		String inputPath = args[0];

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<MyUser> input = env.createInput(new AvroInputFormat<MyUser>(new Path(inputPath), MyUser.class));

		DataSet<Tuple2<String, MyUser>> result = input.map(new NameExtractor()).groupBy(0).reduce(new NameGrouper());

		result.output(new DiscardingOutputFormat<Tuple2<String, MyUser>>());
		env.execute();
	}
}
