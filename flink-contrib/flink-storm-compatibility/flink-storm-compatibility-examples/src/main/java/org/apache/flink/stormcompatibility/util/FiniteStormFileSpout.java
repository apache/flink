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

package org.apache.flink.stormcompatibility.util;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Values;
import org.apache.flink.stormcompatibility.wrappers.FiniteStormSpout;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Implements a Storm Spout that reads data from a given local file. The spout stops automatically
 * when it reached the end of the file.
 */
public class FiniteStormFileSpout extends AbstractStormSpout implements FiniteStormSpout {
	private static final long serialVersionUID = -6996907090003590436L;

	private final String path;
	private BufferedReader reader;
	private String line;
	private boolean newLineRead;

	public FiniteStormFileSpout(final String path) {
		this.path = path;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(final Map conf, final TopologyContext context,
			final SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		try {
			this.reader = new BufferedReader(new FileReader(this.path));
		} catch (final FileNotFoundException e) {
			throw new RuntimeException(e);
		}
		newLineRead = false;
	}

	@Override
	public void close() {
		if (this.reader != null) {
			try {
				this.reader.close();
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

	@Override
	public void nextTuple() {
		this.collector.emit(new Values(line));
		newLineRead = false;
	}

	/**
	 * Can be called before nextTuple() any times including 0.
	 */
	public boolean reachedEnd() {
		try {
			readLine();
		} catch (IOException e) {
			throw new RuntimeException("Exception occured while reading file " + path);
		}
		return line == null;
	}

	private void readLine() throws IOException {
		if (!newLineRead) {
			line = reader.readLine();
			newLineRead = true;
		}
	}

}
