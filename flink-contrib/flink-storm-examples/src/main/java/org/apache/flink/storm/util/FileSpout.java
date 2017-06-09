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

package org.apache.flink.storm.util;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Implements a Spout that reads data from a given local file.
 */
public class FileSpout extends AbstractLineSpout {
	private static final long serialVersionUID = -6996907090003590436L;

	public static final String INPUT_FILE_PATH = "input.path";

	protected String path = null;
	protected BufferedReader reader;

	public FileSpout() {}

	public FileSpout(final String path) {
		this.path = path;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		super.open(conf, context, collector);

		Object configuredPath = conf.get(INPUT_FILE_PATH);
		if (configuredPath != null) {
			this.path = (String) configuredPath;
		}

		try {
			this.reader = new BufferedReader(new FileReader(this.path));
		} catch (final FileNotFoundException e) {
			throw new RuntimeException(e);
		}
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
		String line;
		try {
			line = this.reader.readLine();
			if (line != null) {
				this.collector.emit(new Values(line));
			}
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

}
