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

package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.summarize.StringColumnSummary;

/**
 * {@link Aggregator} that calculates statistics for {@link String} values.
 */
@Internal
public class StringSummaryAggregator implements Aggregator<String, StringColumnSummary> {

	private long nonNullCount = 0L;
	private long nullCount = 0L;
	private long emptyCount = 0L;
	private int minStringLength = Integer.MAX_VALUE;
	private int maxStringLength = -1;
	private CompensatedSum meanLength = CompensatedSum.ZERO;

	@Override
	public void aggregate(String value) {
		if (value == null) {
			nullCount++;
		}
		else {
			nonNullCount++;

			if (value.isEmpty()) {
				emptyCount++;
			}

			int length = value.length();

			minStringLength = Math.min(minStringLength, length);
			maxStringLength = Math.max(maxStringLength, length);

			double delta = length - meanLength.value();
			meanLength = meanLength.add(delta / nonNullCount);
		}
	}

	@Override
	public void combine(Aggregator<String, StringColumnSummary> otherSameType) {
		StringSummaryAggregator other = (StringSummaryAggregator) otherSameType;

		nullCount += other.nullCount;

		minStringLength = Math.min(minStringLength, other.minStringLength);
		maxStringLength = Math.max(maxStringLength, other.maxStringLength);

		if (nonNullCount == 0) {
			nonNullCount = other.nonNullCount;
			emptyCount = other.emptyCount;
			meanLength = other.meanLength;

		}
		else if (other.nonNullCount != 0) {
			long combinedCount = nonNullCount + other.nonNullCount;

			emptyCount += other.emptyCount;

			double deltaMean = other.meanLength.value() - meanLength.value();
			meanLength = meanLength.add(deltaMean * other.nonNullCount / combinedCount);
			nonNullCount = combinedCount;
		}
	}

	@Override
	public StringColumnSummary result() {
		return new StringColumnSummary(
			nonNullCount,
			nullCount,
			emptyCount,
			nonNullCount == 0L ? null : minStringLength,
			nonNullCount == 0L ? null : maxStringLength,
			nonNullCount == 0L ? null : meanLength.value()
		);
	}
}
