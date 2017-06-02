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

import java.io.IOException;
import java.util.Map;

/**
 * Implements a Spout that reads data from a given local file. The spout stops automatically
 * when it reached the end of the file.
 */
public class FiniteFileSpout extends FileSpout implements FiniteSpout {
	private static final long serialVersionUID = -1472978008607215864L;

	private String line;
	private boolean newLineRead;

	public FiniteFileSpout() {}

	public FiniteFileSpout(String path) {
		super(path);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(final Map conf, final TopologyContext context, final SpoutOutputCollector collector) {
		super.open(conf, context, collector);
		newLineRead = false;
	}

	@Override
	public void nextTuple() {
		this.collector.emit(new Values(line));
		newLineRead = false;
	}

	/**
	 * Can be called before nextTuple() any times including 0.
	 */
	@Override
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
