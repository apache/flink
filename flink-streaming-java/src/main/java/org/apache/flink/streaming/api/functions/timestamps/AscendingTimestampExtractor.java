/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.timestamps;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * A timestamp assigner and watermark generator for streams where timestamps are monotonously
 * ascending. In this case, the local watermarks for the streams are easy to generate, because
 * they strictly follow the timestamps.
 *
 * @param <T> The type of the elements that this function can extract timestamps from
 */
@PublicEvolving
public abstract class AscendingTimestampExtractor<T> implements AssignerWithPeriodicWatermarks<T> {

	private static final long serialVersionUID = 1L;

	/** The current timestamp. */
	private long currentTimestamp = Long.MIN_VALUE;

	/** Handler that is called when timestamp monotony is violated. */
	private MonotonyViolationHandler violationHandler = new LoggingHandler();


	/**
	 * Extracts the timestamp from the given element. The timestamp must be monotonically increasing.
	 *
	 * @param element The element that the timestamp is extracted from.
	 * @return The new timestamp.
	 */
	public abstract long extractAscendingTimestamp(T element);

	/**
	 * Sets the handler for violations to the ascending timestamp order.
	 *
	 * @param handler The violation handler to use.
	 * @return This extractor.
	 */
	public AscendingTimestampExtractor<T> withViolationHandler(MonotonyViolationHandler handler) {
		this.violationHandler = requireNonNull(handler);
		return this;
	}

	// ------------------------------------------------------------------------

	@Override
	public final long extractTimestamp(T element, long elementPrevTimestamp) {
		final long newTimestamp = extractAscendingTimestamp(element);
		if (newTimestamp >= this.currentTimestamp) {
			this.currentTimestamp = newTimestamp;
			return newTimestamp;
		} else {
			violationHandler.handleViolation(newTimestamp, this.currentTimestamp);
			return newTimestamp;
		}
	}

	@Override
	public final Watermark getCurrentWatermark() {
		return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
	}

	// ------------------------------------------------------------------------
	//  Handling violations of monotonous timestamps
	// ------------------------------------------------------------------------

	/**
	 * Interface for handlers that handle violations of the monotonous ascending timestamps
	 * property.
	 */
	public interface MonotonyViolationHandler extends java.io.Serializable {

		/**
		 * Called when the property of monotonously ascending timestamps is violated, i.e.,
		 * when {@code elementTimestamp < lastTimestamp}.
		 *
		 * @param elementTimestamp The timestamp of the current element.
		 * @param lastTimestamp The last timestamp.
		 */
		void handleViolation(long elementTimestamp, long lastTimestamp);
	}

	/**
	 * Handler that does nothing when timestamp monotony is violated.
	 */
	public static final class IgnoringHandler implements MonotonyViolationHandler {
		private static final long serialVersionUID = 1L;

		@Override
		public void handleViolation(long elementTimestamp, long lastTimestamp) {}
	}

	/**
	 * Handler that fails the program when timestamp monotony is violated.
	 */
	public static final class FailingHandler implements MonotonyViolationHandler {
		private static final long serialVersionUID = 1L;

		@Override
		public void handleViolation(long elementTimestamp, long lastTimestamp) {
			throw new RuntimeException("Ascending timestamps condition violated. Element timestamp "
					+ elementTimestamp + " is smaller than last timestamp " + lastTimestamp);
		}
	}

	/**
	 * Handler that only logs violations of timestamp monotony, on WARN log level.
	 */
	public static final class LoggingHandler implements MonotonyViolationHandler {
		private static final long serialVersionUID = 1L;

		private static final Logger LOG = LoggerFactory.getLogger(AscendingTimestampExtractor.class);

		@Override
		public void handleViolation(long elementTimestamp, long lastTimestamp) {
			LOG.warn("Timestamp monotony violated: {} < {}", elementTimestamp, lastTimestamp);
		}
	}
}
