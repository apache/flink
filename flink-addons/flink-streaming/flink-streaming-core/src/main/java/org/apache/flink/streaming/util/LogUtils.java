/**
 *
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
 *
 */

package org.apache.flink.streaming.util;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class LogUtils {

	public static void initializeDefaultConsoleLogger() {
		initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);
	}

	public static void initializeDefaultConsoleLogger(Level logLevel, Level rootLevel) {
		Logger logger = Logger.getLogger("org.apache.flink.streaming");
		logger.removeAllAppenders();
		logger.setAdditivity(false);
		PatternLayout layout = new PatternLayout();
		// layout.setConversionPattern("%highlight{%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n}");
		// TODO Add highlight
		layout.setConversionPattern("%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n");
		ConsoleAppender appender = new ConsoleAppender(layout, "System.err");
		logger.addAppender(appender);
		logger.setLevel(logLevel);

		Logger root = Logger.getRootLogger();
		root.removeAllAppenders();
		root.addAppender(appender);
		root.setLevel(rootLevel);
	}
}
