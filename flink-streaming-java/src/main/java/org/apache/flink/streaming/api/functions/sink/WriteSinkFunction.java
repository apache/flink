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

package org.apache.flink.streaming.api.functions.sink;

import org.apache.flink.annotation.PublicEvolving;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;

/**
 * Simple implementation of the SinkFunction writing tuples as simple text to
 * the file specified by path. Tuples are collected to a list and written to the
 * file periodically. The file specified by path is created if it does not
 * exist, cleared if it exists before the writing.
 *
 * @param <IN>
 *            Input tuple type
 *
 * @deprecated Please use the {@link org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink StreamingFileSink}
 * for writing to files from a streaming program.
 */
@PublicEvolving
@Deprecated
public abstract class WriteSinkFunction<IN> implements SinkFunction<IN> {
	private static final long serialVersionUID = 1L;

	protected final String path;
	protected ArrayList<IN> tupleList = new ArrayList<IN>();
	protected WriteFormat<IN> format;

	public WriteSinkFunction(String path, WriteFormat<IN> format) {
		this.path = path;
		this.format = format;
		cleanFile(path);
	}

	/**
	 * Creates target file if it does not exist, cleans it if it exists.
	 *
	 * @param path
	 *            is the path to the location where the tuples are written
	 */
	protected void cleanFile(String path) {
		try {
			PrintWriter writer;
			writer = new PrintWriter(path);
			writer.print("");
			writer.close();
		} catch (FileNotFoundException e) {
			throw new RuntimeException("An error occurred while cleaning the file: " + e.getMessage(), e);
		}
	}

	/**
	 * Condition for writing the contents of tupleList and clearing it.
	 *
	 * @return value of the updating condition
	 */
	protected abstract boolean updateCondition();

	/**
	 * Statements to be executed after writing a batch goes here.
	 */
	protected abstract void resetParameters();

	/**
	 * Implementation of the invoke method of the SinkFunction class. Collects
	 * the incoming tuples in tupleList and appends the list to the end of the
	 * target file if updateCondition() is true or the current tuple is the
	 * endTuple.
	 */
	@Override
	public void invoke(IN tuple) {

		tupleList.add(tuple);
		if (updateCondition()) {
			format.write(path, tupleList);
			resetParameters();
		}

	}

}
