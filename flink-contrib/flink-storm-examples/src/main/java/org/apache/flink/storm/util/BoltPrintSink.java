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

import backtype.storm.task.TopologyContext;

import java.util.Map;

/**
 * Implements a sink that prints the received data to {@code stdout}.
 */
public final class BoltPrintSink extends AbstractBoltSink {
	private static final long serialVersionUID = -6650011223001009519L;

	public BoltPrintSink(OutputFormatter formatter) {
		super(formatter);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepareSimple(final Map stormConf, final TopologyContext context) {
		/* nothing to do */
	}

	@Override
	public void writeExternal(final String line) {
		System.out.println(line);
	}

}
