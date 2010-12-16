/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.test.jobs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * @author Erik Nijkamp
 */
public class WordCount {
	public static class Text implements Key, Value {
		String value = "";

		public Text() {

		}

		public Text(String value) {
			this.value = value;
		}

		public void setValue(String value) {
			this.value = value;
		}

		@Override
		public void read(DataInput in) throws IOException {
			value = in.readUTF();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeUTF(value);
		}

		public boolean equals(Object o) {
			return ((Text) o).value.equals(value);
		}

		@Override
		public int compareTo(Key other) {
			return value.compareTo(((Text) other).value);
		}

		@Override
		public String toString() {
			return value;
		}
	}

	public static class Integer implements Key, Value {
		int value = 0;

		public Integer() {

		}

		public Integer(int value) {
			this.value = value;
		}

		@Override
		public void read(DataInput in) throws IOException {
			value = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			out.writeInt(value);
		}

		@Override
		public int compareTo(Key other) {
			Integer key = (Integer) other;
			if (this.value > key.value) {
				return +1;
			} else if (this.value < key.value) {
				return -1;
			} else {
				return 0;
			}
		}

		@Override
		public String toString() {
			return value + "";
		}
	}

	public static class TextFormatIn extends TextInputFormat<Text, Text> {
		@Override
		public boolean readLine(KeyValuePair<Text, Text> text, byte[] line) {
			StringBuffer buffer = new StringBuffer();
			for (int i = 0; i < line.length; i++) {
				buffer.append((char) line[i]);
			}
			text.getValue().setValue(buffer.toString());
			System.out.println(">>>>>>>>>>>>> read (" + this + ") : " + text.getValue().value);
			return true;
		}

		@Override
		public KeyValuePair<Text, Text> createPair() {
			return new KeyValuePair<Text, Text>(new Text(), new Text());
		}
	}

	public static class TextFormatOut extends TextOutputFormat<Text, Integer> {
		@Override
		public byte[] writeLine(KeyValuePair<Text, Integer> pair) {
			System.out.println(">>>>>>>>>> write (" + this + ") : " + pair.toString());
			return (pair.getKey().toString() + ":" + pair.getValue().toString() + "\n").getBytes();
		}

		@Override
		public KeyValuePair<Text, Integer> createPair() {
			return new KeyValuePair<Text, Integer>(new Text(), new Integer());
		}
	}

	public static class Mapper extends MapStub<Text, Text, Text, Integer> {
		protected void map(Text key, Text value, Collector<Text, Integer> out) {
			System.out.println(">>>>>>>>>>>>> map (" + this + ") : " + value.value);
			out.collect(value, new Integer(1));
		}
	}

	public static class Reducer extends ReduceStub<Text, Integer, Text, Integer> {
		public void reduce(Text key, Iterator<Integer> values, Collector<Text, Integer> out) {
			System.out.println(">>>>>>>>>>>>> reduce (" + this + ") : " + key.value);
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().value;
			}
			out.collect(key, new Integer(sum));
		}
	}
}
