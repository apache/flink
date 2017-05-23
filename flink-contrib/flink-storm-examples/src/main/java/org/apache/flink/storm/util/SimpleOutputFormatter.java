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

package org.apache.flink.storm.util;

import org.apache.storm.tuple.Tuple;

/**
 * Simple {@link OutputFormatter} implementation to convert {@link Tuple Tuples} with a size of 1 by returning the
 * result of {@link Object#toString()} for the first field.
 */
public class SimpleOutputFormatter implements OutputFormatter {
	private static final long serialVersionUID = 6349573860144270338L;

	/**
	 * Converts a Storm {@link Tuple} with 1 field to a string by retrieving the value of that field. This method is
	 * used for formatting raw outputs wrapped in tuples, before writing them out to a file or to the console.
	 *
	 * @param input
	 *            The tuple to be formatted
	 * @return The string result of the formatting
	 */
	@Override
	public String format(final Tuple input) {
		if (input.getValues().size() != 1) {
			throw new RuntimeException("The output is not raw");
		}
		return input.getValue(0).toString();
	}
}
