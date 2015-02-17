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

package org.apache.flink.graph.example.utils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

public class ExampleUtils {

	@SuppressWarnings({ "serial", "unchecked", "rawtypes" })
	public static void printResult(DataSet set, String msg) {
		set.output(new PrintingOutputFormatWithMessage(msg) {
		});
	}

	public static class PrintingOutputFormatWithMessage<T> implements
			OutputFormat<T> {

		private static final long serialVersionUID = 1L;

		private transient PrintStream stream;

		private transient String prefix;

		private String message;

		// --------------------------------------------------------------------------------------------

		/**
		 * Instantiates a printing output format that prints to standard out.
		 */
		public PrintingOutputFormatWithMessage() {
		}

		public PrintingOutputFormatWithMessage(String msg) {
			this.message = msg;
		}

		@Override
		public void open(int taskNumber, int numTasks) {
			// get the target stream
			this.stream = System.out;

			// set the prefix to message
			this.prefix = message + ": ";
		}

		@Override
		public void writeRecord(T record) {
			if (this.prefix != null) {
				this.stream.println(this.prefix + record.toString());
			} else {
				this.stream.println(record.toString());
			}
		}

		@Override
		public void close() {
			this.stream = null;
			this.prefix = null;
		}

		@Override
		public String toString() {
			return "Print to System.out";
		}

		@Override
		public void configure(Configuration parameters) {
		}
	}

	@SuppressWarnings("serial")
	public static DataSet<Vertex<Long, NullValue>> getVertexIds(
			ExecutionEnvironment env, final long numVertices) {
		return env.generateSequence(1, numVertices).map(
				new MapFunction<Long, Vertex<Long, NullValue>>() {
					public Vertex<Long, NullValue> map(Long l) {
						return new Vertex<Long, NullValue>(l, NullValue
								.getInstance());
					}
				});
	}

	@SuppressWarnings("serial")
	public static DataSet<Edge<Long, NullValue>> getRandomEdges(
			ExecutionEnvironment env, final long numVertices) {
		return env.generateSequence(1, numVertices).flatMap(
				new FlatMapFunction<Long, Edge<Long, NullValue>>() {
					@Override
					public void flatMap(Long key,
							Collector<Edge<Long, NullValue>> out)
							throws Exception {
						int numOutEdges = (int) (Math.random() * (numVertices / 2));
						for (int i = 0; i < numOutEdges; i++) {
							long target = (long) (Math.random() * numVertices) + 1;
							out.collect(new Edge<Long, NullValue>(key, target,
									NullValue.getInstance()));
						}
					}
				});
	}

}
