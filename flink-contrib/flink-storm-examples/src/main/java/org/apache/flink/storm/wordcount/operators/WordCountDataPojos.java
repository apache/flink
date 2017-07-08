/*
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
 */

package org.apache.flink.storm.wordcount.operators;

import org.apache.flink.storm.wordcount.util.WordCountData;

import java.io.Serializable;

/**
 * Input POJOs for WordCount programs.
 */
public class WordCountDataPojos {
	public static final Sentence[] SENTENCES;

	static {
		SENTENCES = new Sentence[WordCountData.WORDS.length];
		for (int i = 0; i < SENTENCES.length; ++i) {
			SENTENCES[i] = new Sentence(WordCountData.WORDS[i]);
		}
	}

	/**
	 * Simple POJO containing a string.
	 */
	public static class Sentence implements Serializable {
		private static final long serialVersionUID = -7336372859203407522L;

		private String sentence;

		public Sentence() {
		}

		public Sentence(String sentence) {
			this.sentence = sentence;
		}

		public String getSentence() {
			return sentence;
		}

		public void setSentence(String sentence) {
			this.sentence = sentence;
		}

		@Override
		public String toString() {
			return "(" + this.sentence + ")";
		}
	}
}
