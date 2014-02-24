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
//package eu.stratosphere.api.avro;
//
//
//import java.io.Serializable;
//import java.util.Collections;
//import java.util.Iterator;
//import java.util.List;
//
//import eu.stratosphere.api.common.Plan;
//import eu.stratosphere.api.common.Program;
//import eu.stratosphere.api.common.io.OutputFormat;
//import eu.stratosphere.api.common.operators.FileDataSource;
//import eu.stratosphere.api.common.operators.GenericDataSink;
//import eu.stratosphere.api.java.record.functions.MapFunction;
//import eu.stratosphere.api.java.record.functions.ReduceFunction;
//import eu.stratosphere.api.java.record.io.avro.AvroInputFormat;
//import eu.stratosphere.api.java.record.operators.MapOperator;
//import eu.stratosphere.api.java.record.operators.ReduceOperator;
//import eu.stratosphere.client.LocalExecutor;
//import eu.stratosphere.configuration.Configuration;
//import eu.stratosphere.types.Record;
//import eu.stratosphere.types.StringValue;
//import eu.stratosphere.util.Collector;
//
//
//public class AvroExternalJarProgram implements Program {
//	
//	private static final long serialVersionUID = 1L;
//
//	@Override
//	public Plan getPlan(String... args) {
//		String inputPath = args[0];
//		
//		FileDataSource source = new FileDataSource(new AvroInputFormat<MyUser>(SUser.class), inputPath);
//		
//		MapOperator mapper = MapOperator.builder(new NameExtractor()).input(source).build();
//		
//		ReduceOperator reducer = ReduceOperator.builder(new NameGrouper(), StringValue.class, 0).input(mapper).build();
//		
//		GenericDataSink sink = new GenericDataSink(new DiscardingOutputFormat(), reducer);
//		
//		Plan p = new Plan(sink);
//		p.setDefaultParallelism(4);
//		
//		return p;
//	}
//	
//	// --------------------------------------------------------------------------------------------
//	
//	// --------------------------------------------------------------------------------------------
//	
//	public static final class Color {
//		
//		private String name;
//		private double saturation;
//		
//		public Color() {
//			name = "";
//			saturation = 1.0;
//		}
//		
//		public Color(String name, double saturation) {
//			this.name = name;
//			this.saturation = saturation;
//		}
//		
//		public String getName() {
//			return name;
//		}
//		
//		public void setName(String name) {
//			this.name = name;
//		}
//		
//		public double getSaturation() {
//			return saturation;
//		}
//		
//		public void setSaturation(double saturation) {
//			this.saturation = saturation;
//		}
//	}
//	
//	public static final class MyUser {
//		
//		private String name;
//		private List<Color> colors;
//		
//		public MyUser() {
//			name = "unknown";
//			colors = Collections.emptyList();
//		}
//		
//		public MyUser(String name, List<Color> colors) {
//			this.name = name;
//			this.colors = colors;
//		}
//		
//		public String getName() {
//			return name;
//		}
//		
//		public List<Color> getColors() {
//			return colors;
//		}
//		
//		public void setName(String name) {
//			this.name = name;
//		}
//		
//		public void setColors(List<Color> colors) {
//			this.colors = colors;
//		}
//	}
//	
//	
//	public static final class SUser extends AvroBaseValue<MyUser> {
//		
//		static final long serialVersionUID = 1L;
//
//		public SUser() {}
//	
//		public SUser(MyUser u) {
//			super(u);
//		}
//	}
//	
//	// --------------------------------------------------------------------------------------------
//	
//	// --------------------------------------------------------------------------------------------
//	
//	public static final class NameExtractor extends MapFunction implements Serializable {
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public void map(Record record, Collector<Record> out) throws Exception {
//			SUser su = record.getField(0, SUser.class);
//			MyUser u = su.datum();
//			
//			String namePrefix = u.getName().substring(0, 1);
//			out.collect(new Record(new StringValue(namePrefix), su));
//		}
//		
//	}
//	
//	public static final class NameGrouper extends ReduceFunction implements Serializable {
//		private static final long serialVersionUID = 1L;
//
//		@Override
//		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
//			out.collect(records.next());
//		}
//		
//	}
//	
//	public static final class DiscardingOutputFormat implements OutputFormat<Record> {
//
//		private static final long serialVersionUID = 1L;
//				
//		
//		@Override
//		public void configure(Configuration parameters) {}
//
//		@Override
//		public void open(int taskNumber, int numTasks) {}
//
//		@Override
//		public void writeRecord(Record element) {
//			element.getField(0, StringValue.class);
//			element.getField(1, SUser.class);
//		}
//		
//		@Override
//		public void close() {}
//	}
//	
//
//	// --------------------------------------------------------------------------------------------
//	//  Test Data
//	// --------------------------------------------------------------------------------------------
//	
//	public static final class Generator {
//		
//		private final Random rnd = new Random(2389756789345689276L);
//		
//		public MyUser nextUser() {
//			return randomUser();
//		}
//		
//		private MyUser randomUser() {
//			
//			int numColors = rnd.nextInt(5);
//			ArrayList<Color> colors = new ArrayList<Color>(numColors);
//			for (int i = 0; i < numColors; i++) {
//				colors.add(new Color(randomString(), rnd.nextDouble()));
//			}
//			
//			return new MyUser(randomString(), colors);
//		}
//		
//		private String randomString() {
//			char[] c = new char[this.rnd.nextInt(20) + 5];
//			
//			for (int i = 0; i < c.length; i++) {
//				c[i] = (char) (this.rnd.nextInt(150) + 40);
//			}
//			
//			return new String(c);
//		}
//	}
//	
//	public static void writeTestData(File testFile, int numRecords) throws IOException {
//		
//		DatumWriter<MyUser> userDatumWriter = new ReflectDatumWriter<MyUser>(MyUser.class);
//		DataFileWriter<MyUser> dataFileWriter = new DataFileWriter<MyUser>(userDatumWriter);
//		
//		dataFileWriter.create(ReflectData.get().getSchema(MyUser.class), testFile);
//		
//		
//		Generator generator = new Generator();
//		
//		for (int i = 0; i < numRecords; i++) {
//			MyUser user = generator.nextUser();
//			dataFileWriter.append(user);
//		}
//		
//		dataFileWriter.close();
//	}
//
//	public static void main(String[] args) throws Exception {
//		String testDataFile = new File("src/test/resources/testdata.avro").getAbsolutePath();
//		writeTestData(new File(testDataFile), 50);
//	}
//	
//	public static void main(String[] args) throws Exception {
//		String testDataFile = AvroExternalJarProgram.class.getResource("/testdata.avro").toString();
//		LocalExecutor.execute(new AvroExternalJarProgram(), testDataFile);
//	}
//}
