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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.util.Preconditions;

import java.math.MathContext;
import java.util.TimeZone;

/**
 * A config to define the runtime behavior of the Table API.
 */
@PublicEvolving
public class TableConfig {

	/**
	 * Defines the timezone for date/time/timestamp conversions.
	 */
	private TimeZone timeZone = TimeZone.getTimeZone("UTC");

	/**
	 * Defines if all fields need to be checked for NULL first.
	 */
	private Boolean nullCheck = true;

	/**
	 * Defines the configuration of Planner for Table API and SQL queries.
	 */
	private PlannerConfig plannerConfig = PlannerConfig.EMPTY_CONFIG;

	/**
	 * Defines the default context for decimal division calculation.
	 * We use Scala's default MathContext.DECIMAL128.
	 */
	private MathContext decimalContext = MathContext.DECIMAL128;

	/**
	 * Specifies a threshold where generated code will be split into sub-function calls. Java has a
	 * maximum method length of 64 KB. This setting allows for finer granularity if necessary.
	 */
	private Integer maxGeneratedCodeLength = 64000; // just an estimate

	/**
	 * The minimum time until state which was not updated will be retained.
	 * State might be cleared and removed if it was not updated for the defined period of time.
	 */
	private long minIdleStateRetentionTime = 0L;

	/**
	 * The maximum time until state which was not updated will be retained.
	 * State will be cleared and removed if it was not updated for the defined period of time.
	 */
	private long maxIdleStateRetentionTime = 0L;

	/**
	 * Returns the timezone for date/time/timestamp conversions.
	 */
	public TimeZone getTimeZone() {
		return timeZone;
	}

	/**
	 * Sets the timezone for date/time/timestamp conversions.
	 */
	public void setTimeZone(TimeZone timeZone) {
		this.timeZone = Preconditions.checkNotNull(timeZone);
	}

	/**
	 * Returns the NULL check. If enabled, all fields need to be checked for NULL first.
	 */
	public Boolean getNullCheck() {
		return nullCheck;
	}

	/**
	 * Sets the NULL check. If enabled, all fields need to be checked for NULL first.
	 */
	public void setNullCheck(Boolean nullCheck) {
		this.nullCheck = Preconditions.checkNotNull(nullCheck);
	}

	/**
	 * Returns the current configuration of Planner for Table API and SQL queries.
	 */
	public PlannerConfig getPlannerConfig() {
		return plannerConfig;
	}

	/**
	 * Sets the configuration of Planner for Table API and SQL queries.
	 * Changing the configuration has no effect after the first query has been defined.
	 */
	public void setPlannerConfig(PlannerConfig plannerConfig) {
		this.plannerConfig = Preconditions.checkNotNull(plannerConfig);
	}

	/**
	 * Returns the default context for decimal division calculation.
	 * {@link java.math.MathContext#DECIMAL128} by default.
	 */
	public MathContext getDecimalContext() {
		return decimalContext;
	}

	/**
	 * Sets the default context for decimal division calculation.
	 * {@link java.math.MathContext#DECIMAL128} by default.
	 */
	public void setDecimalContext(MathContext decimalContext) {
		this.decimalContext = Preconditions.checkNotNull(decimalContext);
	}

	/**
	 * Returns the current threshold where generated code will be split into sub-function calls.
	 * Java has a maximum method length of 64 KB. This setting allows for finer granularity if
	 * necessary. Default is 64000.
	 */
	public Integer getMaxGeneratedCodeLength() {
		return maxGeneratedCodeLength;
	}

	/**
	 * Returns the current threshold where generated code will be split into sub-function calls.
	 * Java has a maximum method length of 64 KB. This setting allows for finer granularity if
	 * necessary. Default is 64000.
	 */
	public void setMaxGeneratedCodeLength(Integer maxGeneratedCodeLength) {
		this.maxGeneratedCodeLength = Preconditions.checkNotNull(maxGeneratedCodeLength);
	}

	/**
	 * Specifies a minimum and a maximum time interval for how long idle state, i.e., state which
	 * was not updated, will be retained.
	 * State will never be cleared until it was idle for less than the minimum time and will never
	 * be kept if it was idle for more than the maximum time.
	 *
	 * <p>When new data arrives for previously cleaned-up state, the new data will be handled as if it
	 * was the first data. This can result in previous results being overwritten.
	 *
	 * <p>Set to 0 (zero) to never clean-up the state.
	 *
	 * <p>NOTE: Cleaning up state requires additional bookkeeping which becomes less expensive for
	 * larger differences of minTime and maxTime. The difference between minTime and maxTime must be
	 * at least 5 minutes.
	 *
	 * @param minTime The minimum time interval for which idle state is retained. Set to 0 (zero) to
	 *                never clean-up the state.
	 * @param maxTime The maximum time interval for which idle state is retained. Must be at least
	 *                5 minutes greater than minTime. Set to 0 (zero) to never clean-up the state.
	 */
	public void setIdleStateRetentionTime(Time minTime, Time maxTime) {
		if (maxTime.toMilliseconds() - minTime.toMilliseconds() < 300000 &&
			!(maxTime.toMilliseconds() == 0 && minTime.toMilliseconds() == 0)) {
			throw new IllegalArgumentException(
				"Difference between minTime: " + minTime.toString() + " and maxTime: " + maxTime.toString() +
					"shoud be at least 5 minutes.");
		}
		minIdleStateRetentionTime = minTime.toMilliseconds();
		maxIdleStateRetentionTime = maxTime.toMilliseconds();
	}

	/**
	 * @return The minimum time until state which was not updated will be retained.
	 */
	public long getMinIdleStateRetentionTime() {
		return minIdleStateRetentionTime;
	}

	/**
	 * @return The maximum time until state which was not updated will be retained.
	 */
	public long getMaxIdleStateRetentionTime() {
		return maxIdleStateRetentionTime;
	}

	public static TableConfig getDefault() {
		return new TableConfig();
	}
}
