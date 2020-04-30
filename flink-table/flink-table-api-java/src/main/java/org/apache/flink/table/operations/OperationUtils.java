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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper methods for {@link Operation}s.
 */
@Internal
public class OperationUtils {

	private static final String OPERATION_INDENT = "    ";

	/**
	 * Increases indentation for description of string of child {@link Operation}.
	 * The input can already contain indentation. This will increase all the indentations
	 * by one level.
	 *
	 * @param item result of {@link Operation#asSummaryString()}
	 * @return string with increased indentation
	 */
	static String indent(String item) {
		return "\n" + OPERATION_INDENT +
			item.replace("\n" + OPERATION_INDENT, "\n" + OPERATION_INDENT + OPERATION_INDENT);
	}

	/**
	 * Formats a Tree of {@link Operation} in a unified way. It prints all the parameters and
	 * adds all children formatted and properly indented in the following lines.
	 *
	 * <p>The format is <pre>
	 * {@code
	 * <operationName>: [(key1: [value1], key2: [v1, v2])]
	 *     <child1>
	 *          <child2>
	 *     <child3>
	 * }
	 * </pre>
	 *
	 * @param operationName The operation name.
	 * @param parameters The operation's parameters.
	 * @param children The operation's children.
	 * @param childToString The function to convert child to String.
	 * @param <T> The type of the child.
	 * @return String representation of the given operation.
	 */
	public static <T extends Operation> String formatWithChildren(
			String operationName,
			Map<String, Object> parameters,
			List<T> children,
			Function<T, String> childToString) {
		String description = parameters.entrySet()
			.stream()
			.map(entry -> formatParameter(entry.getKey(), entry.getValue()))
			.collect(Collectors.joining(", "));

		final StringBuilder stringBuilder = new StringBuilder();

		stringBuilder.append(operationName).append(":");

		if (!StringUtils.isNullOrWhitespaceOnly(description)) {
			stringBuilder.append(" (").append(description).append(")");
		}

		String childrenDescription = children.stream()
			.map(child -> OperationUtils.indent(childToString.apply(child)))
			.collect(Collectors.joining());

		return stringBuilder.append(childrenDescription).toString();
	}

	public static String formatParameter(String name, Object value) {
		final StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(name);
		stringBuilder.append(": ");
		if (value.getClass().isArray()) {
			stringBuilder.append(Arrays.toString((Object[]) value));
		} else if (value instanceof Collection) {
			stringBuilder.append(value);
		} else {
			stringBuilder.append("[").append(value).append("]");
		}
		return stringBuilder.toString();
	}

	private OperationUtils() {
	}
}
