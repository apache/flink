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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;

/**
 * The logging layout used to prefix each log event with the Maven fork number.
 * <p>
 * Use this layout when running tests via Maven in parallel and logging to the Console. When logging
 * to a file, you can use a separate file for each fork.
 */
@Internal
public class MavenForkNumberPrefixLayout extends PatternLayout {

	/** Property name used to set fork number of the forked JVM. */
	private static final String PROPERTY = "mvn.forkNumber";

	private final int prefixLength;

	private final StringBuilder stringBuilder;

	public MavenForkNumberPrefixLayout() {
		super();

		String prefix = System.getProperty(PROPERTY);

		if (prefix != null) {
			prefix += " > ";

			prefixLength = prefix.length();

			stringBuilder = new StringBuilder(512);
			stringBuilder.append(prefix);
		}
		else {
			prefixLength = 0;
			stringBuilder = null;
		}
	}

	@Override
	public String format(LoggingEvent event) {
		if (prefixLength == 0) {
			return super.format(event);
		}

		stringBuilder.setLength(prefixLength);

		stringBuilder.append(super.format(event));

		return stringBuilder.toString();
	}
}
