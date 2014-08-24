/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.api;

import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.LogUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class WriteAsCsvTest {
	
	private static final String PREFIX = System.getProperty("java.io.tmpdir") + "/" + WriteAsCsvTest.class.getSimpleName() + "_";
	
	private static final long MEMORYSIZE = 32;

	private static List<String> result1 = new ArrayList<String>();
	private static List<String> result2 = new ArrayList<String>();
	private static List<String> result3 = new ArrayList<String>();
	private static List<String> result4 = new ArrayList<String>();
	private static List<String> result5 = new ArrayList<String>();

	private static List<String> expected1 = new ArrayList<String>();
	private static List<String> expected2 = new ArrayList<String>();
	private static List<String> expected3 = new ArrayList<String>();
	private static List<String> expected4 = new ArrayList<String>();
	private static List<String> expected5 = new ArrayList<String>();

	public static final class MySource1 implements SourceFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector) throws Exception {
			for (int i = 0; i < 27; i++) {
				collector.collect(new Tuple1<Integer>(i));
			}
		}
	}

	private static void readFile(String path, List<String> result) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line;
			line = br.readLine();
			while (line != null) {
				result.add(line);
				line = br.readLine();
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void fillExpected1() {
		for (int i = 0; i < 27; i++) {
			expected1.add(i + "");
		}
	}

	private static void fillExpected2() {
		for (int i = 0; i < 25; i++) {
			expected2.add(i + "");
		}
	}

	private static void fillExpected3() {
		for (int i = 0; i < 20; i++) {
			expected3.add(i + "");
		}
	}

	private static void fillExpected4() {
		for (int i = 0; i < 26; i++) {
			expected4.add(i + "");
		}
	}

	private static void fillExpected5() {
		for (int i = 0; i < 14; i++) {
			expected5.add(i + "");
		}

		for (int i = 15; i < 25; i++) {
			expected5.add(i + "");
		}
	}

	@BeforeClass
	public static void createFileCleanup() {
		Runnable r = new Runnable() {
			
			@Override
			public void run() {
				try { new File(PREFIX + "test1.txt").delete(); } catch (Throwable t) {}
				try { new File(PREFIX + "test2.txt").delete(); } catch (Throwable t) {}
				try { new File(PREFIX + "test3.txt").delete(); } catch (Throwable t) {}
				try { new File(PREFIX + "test4.txt").delete(); } catch (Throwable t) {}
				try { new File(PREFIX + "test5.txt").delete(); } catch (Throwable t) {}
			}
		};
		
		Runtime.getRuntime().addShutdownHook(new Thread(r));
	}
	
	@Test
	public void test() throws Exception {

		LogUtils.initializeDefaultTestConsoleLogger();
		
		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> dataStream1 = env.addSource(new MySource1(), 1).writeAsCsv(PREFIX + "test1.txt");

		fillExpected1();

		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> dataStream2 = env.addSource(new MySource1(), 1).writeAsCsv(PREFIX + "test2.txt", 5);

		fillExpected2();

		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> dataStream3 = env.addSource(new MySource1(), 1).writeAsCsv(PREFIX + "test3.txt", 10);

		fillExpected3();

		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> dataStream4 = env.addSource(new MySource1(), 1).writeAsCsv(PREFIX + "test4.txt", 10, new Tuple1<Integer>(26));

		fillExpected4();

		@SuppressWarnings("unused")
		DataStream<Tuple1<Integer>> dataStream5 = env.addSource(new MySource1(), 1).writeAsCsv(PREFIX + "test5.txt", 10, new Tuple1<Integer>(14));

		fillExpected5();

		env.executeTest(MEMORYSIZE);

		readFile(PREFIX + "test1.txt", result1);
		readFile(PREFIX + "test2.txt", result2);
		readFile(PREFIX + "test3.txt", result3);
		readFile(PREFIX + "test4.txt", result4);
		readFile(PREFIX + "test5.txt", result5);

		assertTrue(expected1.equals(result1));
		assertTrue(expected2.equals(result2));
		assertTrue(expected3.equals(result3));
		assertTrue(expected4.equals(result4));
		assertTrue(expected5.equals(result5));
	}
}
