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
 * SKIP_PAST_LAST_EVENT (start a new match process after the last event of the current match),
 * SKIP_TO_NEXT_EVENT (start a new match process after the first event of the current match),
 * SKIP_TO_FIRST_<code>PATTERN</code> (start a new match process at the first event of the matched *pattern*) and
 * SKIP_TO_LAST_<code>PATTERN</code> (start a new match process at the last event of the matched *pattern*).
 * </p>
 */
public class AfterMatchSkipStrategy implements Serializable {

	private static final long serialVersionUID = -4048930333619068531L;
	// default strategy
	private SkipStrategy strategy = SkipStrategy.SKIP_TO_NEXT_EVENT;

	// pattern name to skip to
	private String patternName = null;

	/**
	 * Start a new match process at the first event of the matched *PatternName*.
	 * @param patternName the pattern name to skip to
	 * @return the created AfterMatchSkipStrategy
	 */
	public static AfterMatchSkipStrategy skipToFirst(String patternName) {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_TO_FIRST, patternName);
	}

	/**
	 * Start a new match process at the last event of the matched *PatternName*
	 * @param patternName the pattern name to skip to
	 * @return the created AfterMatchSkipStrategy
	 */
	public static AfterMatchSkipStrategy skipToLast(String patternName) {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_TO_LAST, patternName);
	}

	/**
	 * Start a new match process after the last event of the current match.
	 * @return the created AfterMatchSkipStrategy
	 */
	public static AfterMatchSkipStrategy skipPastLastEvent() {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_PAST_LAST_EVENT);
	}

	/**
	 * Start a new match process after the first event of the current match.
	 * @return the created AfterMatchSkipStrategy
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

	/**
	 * Get the {@SkipStrategy} enum.
	 * @return the skip strategy
	 */
	public SkipStrategy getStrategy() {
		return strategy;
	}

	/**
	 * Get the referenced pattern name of this strategy.
	 * @return the referenced pattern name.
	 */
	public String getPatternName() {
		return patternName;
	}

	@Override
	public String toString() {
		switch (strategy) {
			case SKIP_TO_NEXT_EVENT:
			case SKIP_PAST_LAST_EVENT:
				return "AfterMatchStrategy{" +
					strategy +
					"}";
			case SKIP_TO_FIRST:
			case SKIP_TO_LAST:
				return "AfterMatchStrategy{" +
					strategy + "[" +
					patternName + "]" +
					"}";
		}
		return super.toString();
	}

	/**
	 * Skip Strategy Enum.
	 */
	public enum SkipStrategy{
		/**
		 * Start a new match process after the first event of the current match.
		 */
		SKIP_TO_NEXT_EVENT,
		/**
		 * Start a new match process after the last event of the current match.
		 */
		SKIP_PAST_LAST_EVENT,
		/**
		 * Start a new match process at the first event of the matched *PatternName*.
		 */
		SKIP_TO_FIRST,
		/**
		 * Start a new match process at the last event of the matched *PatternName*
		 */
		SKIP_TO_LAST
	}
}
