/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.util;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


public class LogUtils {

	public static void initializeDefaultConsoleLogger() {
		initializeDefaultConsoleLogger(Level.INFO);
	}
	
	public static void initializeDefaultTestConsoleLogger() {
		initializeDefaultConsoleLogger(Level.WARN);
	}
	
	public static void initializeDefaultConsoleLogger(Level logLevel) {
		Logger root = Logger.getRootLogger();
		root.removeAllAppenders();
		PatternLayout layout = new PatternLayout("%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n");
		ConsoleAppender appender = new ConsoleAppender(layout, "System.err");
		root.addAppender(appender);
		root.setLevel(logLevel);
	}
}
