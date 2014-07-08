package eu.stratosphere.api.avro.testjar;
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

// ================================================================================================
//  This file defines the classes for the AvroExternalJarProgramITCase.
//  The program is exported into src/test/resources/AvroTestProgram.jar.
//
//  THIS FILE MUST STAY FULLY COMMENTED SUCH THAT THE HERE DEFINED CLASSES ARE NOT COMPILED
//  AND ADDED TO THE test-classes DIRECTORY. OTHERWISE, THE EXTERNAL CLASS LOADING WILL
//  NOT BE COVERED BY THIS TEST.
// ================================================================================================


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;

import eu.stratosphere.api.avro.AvroBaseValue;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.io.AvroInputFormat;
import eu.stratosphere.api.java.io.DiscardingOuputFormat;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.core.fs.Path;

public class AvroExternalJarProgram  {

	public static final class Color {
		
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
	
	public static final class MyUser {
		
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
	
	
	public static final class SUser extends AvroBaseValue<MyUser> {
		
		static final long serialVersionUID = 1L;

		public SUser() {}
	
		public SUser(MyUser u) {
			super(u);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	// --------------------------------------------------------------------------------------------
	
	public static final class NameExtractor extends MapFunction<MyUser, Tuple2<String, MyUser>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, MyUser> map(MyUser u) {
			String namePrefix = u.getName().substring(0, 1);
			return new Tuple2<String, MyUser>(namePrefix, u);
		}
	}
	
	public static final class NameGrouper extends ReduceFunction<Tuple2<String, MyUser>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<String, MyUser> reduce(Tuple2<String, MyUser> val1, Tuple2<String, MyUser> val2) {
			return val1;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Test Data
	// --------------------------------------------------------------------------------------------
	
	public static final class Generator {
		
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
		
		result.output(new DiscardingOuputFormat<Tuple2<String,MyUser>>());
		env.execute();
	}
}
