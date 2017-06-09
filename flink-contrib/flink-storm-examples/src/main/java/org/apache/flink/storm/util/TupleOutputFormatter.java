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
 * {@link OutputFormatter} implementation that converts {@link Tuple Tuples} of arbitrary size to a string. For a given
 * tuple the output is <code>(field1,field2,...,fieldX)</code>.
 */
public class TupleOutputFormatter implements OutputFormatter {
	private static final long serialVersionUID = -599665757723851761L;

	@Override
	public String format(final Tuple input) {
		final StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("(");
		for (final Object attribute : input.getValues()) {
			stringBuilder.append(attribute);
			stringBuilder.append(",");
		}
		stringBuilder.replace(stringBuilder.length() - 1, stringBuilder.length(), ")");
		return stringBuilder.toString();
	}

}
