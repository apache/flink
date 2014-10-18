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
package org.apache.flink.api.java.typeutils.runtime;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Field;

public class FieldAccessMinibenchmark {

	static Field wordDescField;
	static Field wordField;
	static {
		try {
			wordDescField = WC.class.getField("wordDesc");
			wordField = ComplexWordDescriptor.class.getField("word");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static class ComplexWordDescriptor {
		public String word;

		public String getWord() {
			return word;
		}
	}

	public static class WC {
		public int count;
		public ComplexWordDescriptor wordDesc;

		public WC(int c, String s) throws NoSuchFieldException,
				SecurityException {
			this.count = c;
			this.wordDesc = new ComplexWordDescriptor();
			this.wordDesc.word = s;
		}

		public ComplexWordDescriptor getWordDesc() {
			return wordDesc;
		}

	}

	public static int compareCodeGenPublicFields(WC w1, WC w2) {
		return w1.wordDesc.word.compareTo(w2.wordDesc.word);
	}

	public static int compareCodeGenMethods(WC w1, WC w2) {
		return w1.getWordDesc().getWord().compareTo(w2.getWordDesc().getWord());
	}

	public static int compareReflective(WC w1, WC w2)
			throws IllegalArgumentException, IllegalAccessException {
		// get String of w1
		Object wordDesc1 = wordDescField.get(w1);
		String word2cmp1 = (String) wordField.get(wordDesc1);

		// get String of w2
		Object wordDesc2 = wordDescField.get(w2);
		String word2cmp2 = (String) wordField.get(wordDesc2);

		return word2cmp1.compareTo(word2cmp2);
	}

	/**
	 * results on Core i7 2600k
	 * 
	 * 
	 * warming up Code gen 5019 Reflection 20364 Factor = 4.057382
	 */
	public static void main(String[] args) throws NoSuchFieldException,
			SecurityException, IllegalArgumentException, IllegalAccessException {
		final long RUNS = 1000000000L;

		final RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
		String jvm = bean.getVmName() + " - " + bean.getVmVendor() + " - "
				+ bean.getSpecVersion() + '/' + bean.getVmVersion();
		System.err.println("Jvm info : " + jvm);

		WC word0 = new WC(14, "Hallo");
		WC word1 = new WC(3, "Hola");

		System.err.println("warming up");
		for (long i = 0; i < 100000000; i++) {
			compareCodeGenPublicFields(word0, word1);
			compareCodeGenMethods(word0, word1);
			compareReflective(word0, word1);
		}

		System.err.println("Code gen public fields");
		long startTime = System.currentTimeMillis();
		for (long i = 0; i < RUNS; i++) {
			int a = compareCodeGenPublicFields(word0, word1);
			if (a == 0) {
				System.err.println("hah");
			}
		}
		long stopTime = System.currentTimeMillis();
		long elapsedTimeGen = stopTime - startTime;
		System.err.println(elapsedTimeGen);

		System.err.println("Code gen methods");
		startTime = System.currentTimeMillis();
		for (long i = 0; i < RUNS; i++) {
			int a = compareCodeGenPublicFields(word0, word1);
			if (a == 0) {
				System.err.println("hah");
			}
		}
		stopTime = System.currentTimeMillis();
		long elapsedTimeGenMethods = stopTime - startTime;
		System.err.println(elapsedTimeGenMethods);

		System.err.println("Reflection");

		startTime = System.currentTimeMillis();
		for (long i = 0; i < RUNS; i++) {
			int a = compareReflective(word0, word1);
			if (a == 0) {
				System.err.println("hah");
			}
		}
		stopTime = System.currentTimeMillis();
		long elapsedTimeRef = stopTime - startTime;
		System.err.println(elapsedTimeRef);

		System.err.println("Factor vs public = "
				+ (elapsedTimeRef / (float) elapsedTimeGen));
		System.err.println("Factor vs methods = "
				+ (elapsedTimeRef / (float) elapsedTimeGenMethods));
	}
}
