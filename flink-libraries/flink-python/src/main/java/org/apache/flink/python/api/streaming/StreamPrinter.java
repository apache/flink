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
package org.apache.flink.python.api.streaming;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Simple utility class to print all contents of an inputstream to stdout.
 */
public class StreamPrinter extends Thread {
	private final BufferedReader reader;
	private final boolean wrapInException;
	private StringBuilder msg;

	public StreamPrinter(InputStream stream) {
		this(stream, false, null);
	}

	public StreamPrinter(InputStream stream, boolean wrapInException, StringBuilder msg) {
		this.reader = new BufferedReader(new InputStreamReader(stream));
		this.wrapInException = wrapInException;
		this.msg = msg;
	}

	@Override
	public void run() {
		String line;
		try {
			if (wrapInException) {
				while ((line = reader.readLine()) != null) {
					msg.append("\n" + line);
				}
			} else {
				while ((line = reader.readLine()) != null) {
					System.out.println(line);
					System.out.flush();
				}
			}
		} catch (IOException ex) {
		}
	}
}
