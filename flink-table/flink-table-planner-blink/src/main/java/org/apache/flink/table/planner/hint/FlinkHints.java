/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.hint;

import org.apache.calcite.rel.hint.RelHint;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Utility class for Flink hints. */
public abstract class FlinkHints {
	//~ Static fields/initializers ---------------------------------------------

	public static final String HINT_NAME_OPTIONS = "OPTIONS";

	//~ Tools ------------------------------------------------------------------

	/** Returns the OPTIONS hint options from the
	 * given list of table hints {@code tableHints}, never null. */
	public static Map<String, String> getHintedOptions(List<RelHint> tableHints) {
		return tableHints.stream().filter(hint ->
				hint.hintName.equalsIgnoreCase(HINT_NAME_OPTIONS))
				.findFirst()
				.map(hint -> hint.kvOptions)
				.orElse(Collections.emptyMap());
	}

	/**
	 * Merges the dynamic table options from {@code hints} and static table options
	 * from table definition {@code props}.
	 *
	 * <p>The options in {@code hints} would override the ones in {@code props} if
	 * they have the same option key.
	 *
	 * @param hints Dynamic table options, usually from the OPTIONS hint
	 * @param props Static table options defined in DDL or connect API
	 *
	 * @return New options with merged dynamic table options, or the old {@code props}
	 * if there is no dynamic table options
	 */
	public static Map<String, String> mergeTableOptions(
			Map<String, String> hints,
			Map<String, String> props) {
		if (hints.size() == 0) {
			return props;
		}
		Map<String, String> newProps = new HashMap<>();
		newProps.putAll(props);
		newProps.putAll(hints);
		return Collections.unmodifiableMap(newProps);
	}
}
