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

package org.apache.flink.util.log;

import org.slf4j.LoggerFactory;

import java.io.PrintStream;

public class OutErrLoggerUitls {

	private static String STDOUT_APPENDER = "stdout";
	private static String STDERR_APPENDER = "stderr";

	private static String INFO = "INFO";
	private static String ERROR = "ERROR";

	public static void setOutAndErrToLog() {
		setOutToLog(STDOUT_APPENDER, INFO);
		setErrToLog(STDERR_APPENDER, ERROR);
	}

	private static void setOutToLog(String name, String level) {
		System.setOut(new PrintStream(new LoggerStream(LoggerFactory.getLogger(name), level)));
	}

	private static void setErrToLog(String name, String level) {
		System.setErr(new PrintStream(new LoggerStream(LoggerFactory.getLogger(name), level)));
	}
}
