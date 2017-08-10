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

package org.apache.flink.cep.nfa;

import java.io.Serializable;


/**
 * Indicate the skip strategy after a match process.
 * <p>There're four kinds of strategies:
 * SKIP_PAST_LAST_EVENT,
 * SKIP_TO_NEXT_EVENT,
 * SKIP_TO_FIRST_<code>PATTERN</code> and
 * SKIP_TO_LAST_<code>PATTERN</code>.
 * </p>
 */
public class AfterMatchSkipStrategy implements Serializable {

	// default strategy
	SkipStrategy strategy = SkipStrategy.SKIP_TO_NEXT_EVENT;

	// pattern name to skip to
	String patternName = null;

	/**
	 * Skip to first *pattern*.
	 * @param patternName the pattern name to skip to
	 * @return
	 */
	public static AfterMatchSkipStrategy skipToFirst(String patternName) {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_TO_FIRST, patternName);
	}

	/**
	 * Skip to last *pattern*.
	 * @param patternName the pattern name to skip to
	 * @return
	 */
	public static AfterMatchSkipStrategy skipToLast(String patternName) {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_TO_LAST, patternName);
	}

	/**
	 * Skip past last event.
	 * @return
	 */
	public static AfterMatchSkipStrategy skipPastLastEvent() {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_PAST_LAST_EVENT);
	}

	/**
	 * Skip to next event.
	 * @return
	 */
	public static AfterMatchSkipStrategy skipToNextEvent() {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_TO_NEXT_EVENT);
	}

	private AfterMatchSkipStrategy(SkipStrategy strategy) {
		this(strategy, null);
	}

	private AfterMatchSkipStrategy(SkipStrategy strategy, String patternName) {
		if (patternName == null && (strategy == SkipStrategy.SKIP_TO_FIRST || strategy == SkipStrategy.SKIP_TO_LAST)) {
			throw new IllegalArgumentException("The patternName field can not be empty when SkipStrategy is " + strategy);
		}
		this.strategy = strategy;
		this.patternName = patternName;
	}

	public SkipStrategy getStrategy() {
		return strategy;
	}

	public String getPatternName() {
		return patternName;
	}

	@Override
	public String toString() {
		return "AfterMatchStrategy{" +
			"strategy=" + strategy +
			", patternName=" + patternName +
			'}';
	}

	/**
	 * Skip Strategy Enum.
	 */
	public enum SkipStrategy{
		SKIP_TO_NEXT_EVENT,
		SKIP_PAST_LAST_EVENT,
		SKIP_TO_FIRST,
		SKIP_TO_LAST
	}
}
