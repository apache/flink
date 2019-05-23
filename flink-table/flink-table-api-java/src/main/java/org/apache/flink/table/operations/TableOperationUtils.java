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

/**
 * Helper methods for {@link TableOperation}s.
 */
@Internal
public class TableOperationUtils {

	private static final String OPERATION_INDENT = "    ";

	/**
	 * Increases indentation for description of string of child {@link TableOperation}.
	 * The input can already contain indentation. This will increase all the indentations
	 * by one level.
	 *
	 * @param item result of {@link TableOperation#asSummaryString()}
	 * @return string with increased indentation
	 */
	static String indent(String item) {
		return "\n" + OPERATION_INDENT +
			item.replace("\n" + OPERATION_INDENT, "\n" + OPERATION_INDENT + OPERATION_INDENT);
	}

	private TableOperationUtils() {
	}
}
