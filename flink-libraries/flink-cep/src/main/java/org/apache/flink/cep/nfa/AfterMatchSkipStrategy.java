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
 *
 * <p>For more info on possible skip strategies see {@link SkipStrategy}.
 */
public class AfterMatchSkipStrategy implements Serializable {

	private static final long serialVersionUID = -4048930333619068531L;
	// default strategy
	private SkipStrategy strategy = SkipStrategy.NO_SKIP;

	// pattern name to skip to
	private String patternName = null;

	/**
	 * Discards every partial match that contains event of the match preceding the first of *PatternName*.
	 * @param patternName the pattern name to skip to
	 * @return the created AfterMatchSkipStrategy
	 */
	public static AfterMatchSkipStrategy skipToFirst(String patternName) {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_TO_FIRST, patternName);
	}

	/**
	 * Discards every partial match that contains event of the match preceding the last of *PatternName*.
	 * @param patternName the pattern name to skip to
	 * @return the created AfterMatchSkipStrategy
	 */
	public static AfterMatchSkipStrategy skipToLast(String patternName) {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_TO_LAST, patternName);
	}

	/**
	 * Discards every partial match that contains event of the match.
	 * @return the created AfterMatchSkipStrategy
	 */
	public static AfterMatchSkipStrategy skipPastLastEvent() {
		return new AfterMatchSkipStrategy(SkipStrategy.SKIP_PAST_LAST_EVENT);
	}

	/**
	 * Every possible match will be emitted.
	 * @return the created AfterMatchSkipStrategy
	 */
	public static AfterMatchSkipStrategy noSkip() {
		return new AfterMatchSkipStrategy(SkipStrategy.NO_SKIP);
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
	 * Get the {@link SkipStrategy} enum.
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
			case NO_SKIP:
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
		 * Every possible match will be emitted.
		 */
		NO_SKIP,
		/**
		 * Discards every partial match that contains event of the match.
		 */
		SKIP_PAST_LAST_EVENT,
		/**
		 * Discards every partial match that contains event of the match preceding the first of *PatternName*.
		 */
		SKIP_TO_FIRST,
		/**
		 * Discards every partial match that contains event of the match preceding the last of *PatternName*.
		 */
		SKIP_TO_LAST
	}
}
