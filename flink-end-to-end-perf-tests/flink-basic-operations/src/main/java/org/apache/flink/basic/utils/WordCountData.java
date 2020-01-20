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

package org.apache.flink.basic.utils;

import java.util.Random;

/**
 * Provides the default data sets used for the WordCount example program.
 */
public class WordCountData {

	WordCountData() {}

	private static final String chars = "abcdefghijklmnopqrstuvwxyz";

	private static String[] generateData(int wordLength, int seed) {
		Random rand = new Random(seed);
		String[] newWords = new String[10000];
		for (int i = 0; i < newWords.length; i++) {
			StringBuilder str = new StringBuilder();
			for (int j = 0; j < wordLength; j++) {
				str.append(chars.charAt(rand.nextInt(Math.min(26, wordLength))));
			}
			newWords[i] = str.toString();
		}
		return newWords;
	}

	public static String[] getWords(int wordLength, int seed) {
			return generateData(wordLength, seed);
	}
}

