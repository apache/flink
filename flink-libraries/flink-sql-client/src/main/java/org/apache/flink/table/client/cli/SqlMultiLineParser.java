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

package org.apache.flink.table.client.cli;

import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.impl.DefaultParser;
//import org.jline.reader.Parser;

//import java.util.Collections;

/**
 * Multi-line parser for parsing an arbitrary number of SQL lines until a line ends with ';'.
 */
//public class SqlMultiLineParser implements Parser {
public class SqlMultiLineParser extends DefaultParser {
	private static final String EOF_CHARACTER = ";";
	private static final String NEW_LINE_PROMPT = ""; // results in simple '>' output

	@Override
	public ParsedLine parse(String line, int cursor, ParseContext context) {
		if (!line.trim().endsWith(EOF_CHARACTER)
			&& context != ParseContext.COMPLETE) {
			throw new EOFError(
				-1,
				-1,
				"New line without EOF character.",
				NEW_LINE_PROMPT);
		}
		return super.parse(line, cursor, context);
	}
}
