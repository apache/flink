/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.flink.python.api.streaming.util;

import org.apache.flink.configuration.ConfigConstants;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Simple utility class to print all contents of an inputstream to stdout.
 */
public class StreamPrinter implements Runnable {
	private final BufferedReader reader;
	private final AtomicReference<String> output;

	public StreamPrinter(InputStream stream) {
		this(stream, null);
	}

	public StreamPrinter(InputStream stream, AtomicReference<String> output) {
		this.reader = new BufferedReader(new InputStreamReader(stream, ConfigConstants.DEFAULT_CHARSET));
		this.output = output;
	}

	@Override
	public void run() {
		String line;
		if (output != null) {
			StringBuilder msg = new StringBuilder();
			try {
				while ((line = reader.readLine()) != null) {
					msg.append(line);
					msg.append("\n");
				}
			} catch (IOException ignored) {
			} finally {
				output.set(msg.toString());
			}
		} else {
			try {
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
					System.out.flush();
				}
			} catch (IOException ignored) {
			}
		}
	}
}
