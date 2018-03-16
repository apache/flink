/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.aggregation;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType;

import java.io.Serializable;

/**
 * Internal comparator implementation, for use with {@link ComparableAggregator}.
 */
@Internal
public abstract class Comparator implements Serializable {

	private static final long serialVersionUID = 1L;

	public abstract <R> int isExtremal(Comparable<R> o1, R o2);

	public static Comparator getForAggregation(AggregationType type) {
		switch (type) {
		case MAX:
			return new MaxComparator();
		case MIN:
			return new MinComparator();
		case MINBY:
			return new MinByComparator();
		case MAXBY:
			return new MaxByComparator();
		default:
			throw new IllegalArgumentException("Unsupported aggregation type.");
		}
	}

	private static class MaxComparator extends Comparator {

		private static final long serialVersionUID = 1L;

		@Override
		public <R> int isExtremal(Comparable<R> o1, R o2) {
			return o1.compareTo(o2) > 0 ? 1 : 0;
		}

	}

	private static class MaxByComparator extends Comparator {

		private static final long serialVersionUID = 1L;

		@Override
		public <R> int isExtremal(Comparable<R> o1, R o2) {
			int c = o1.compareTo(o2);
			if (c > 0) {
				return 1;
			}
			if (c == 0) {
				return 0;
			} else {
				return -1;
			}
		}

	}

	private static class MinByComparator extends Comparator {

		private static final long serialVersionUID = 1L;

		@Override
		public <R> int isExtremal(Comparable<R> o1, R o2) {
			int c = o1.compareTo(o2);
			if (c < 0) {
				return 1;
			}
			if (c == 0) {
				return 0;
			} else {
				return -1;
			}
		}

	}

	private static class MinComparator extends Comparator {

		private static final long serialVersionUID = 1L;

		@Override
		public <R> int isExtremal(Comparable<R> o1, R o2) {
			return o1.compareTo(o2) < 0 ? 1 : 0;
		}

	}
}
