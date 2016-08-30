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

import org.apache.storm.task.TopologyContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Implements a sink that write the received data to the given file (as a result of {@code Object.toString()} for each
 * attribute).
 */
public final class BoltFileSink extends AbstractBoltSink {
	private static final long serialVersionUID = 2014027288631273666L;

	private final String path;
	private BufferedWriter writer;

	public BoltFileSink(final String path) {
		this(path, new SimpleOutputFormatter());
	}

	public BoltFileSink(final String path, final OutputFormatter formatter) {
		super(formatter);
		this.path = path;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareSimple(final Map stormConf, final TopologyContext context) {
		try {
			this.writer = new BufferedWriter(new FileWriter(this.path));
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void writeExternal(final String line) {
		try {
			this.writer.write(line + "\n");
		} catch (final IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void cleanup() {
		if (this.writer != null) {
			try {
				this.writer.close();
			} catch (final IOException e) {
				throw new RuntimeException(e);
			}
		}
	}

}
