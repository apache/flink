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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Optional;

/**
 * Helper class for {@link ArgumentCount} with constant boundaries.
 *
 * <p>Note: All boundaries of this class are inclusive.
 */
@Internal
public final class ConstantArgumentCount implements ArgumentCount {

	private static final int OPEN_INTERVAL = -1;

	private final int minCount;

	private final int maxCount;

	private ConstantArgumentCount(int minCount, int maxCount) {
		this.minCount = minCount;
		this.maxCount = maxCount;
	}

	public static ArgumentCount of(int count) {
		Preconditions.checkArgument(count >= 0);
		return new ConstantArgumentCount(count, count);
	}

	public static ArgumentCount between(int minCount, int maxCount) {
		Preconditions.checkArgument(minCount <= maxCount);
		Preconditions.checkArgument(minCount >= 0);
		return new ConstantArgumentCount(minCount, maxCount);
	}

	public static ArgumentCount from(int minCount) {
		Preconditions.checkArgument(minCount >= 0);
		return new ConstantArgumentCount(minCount, OPEN_INTERVAL);
	}

	public static ArgumentCount any() {
		return new ConstantArgumentCount(0, OPEN_INTERVAL);
	}

	@Override
	public boolean isValidCount(int count) {
		return count >= minCount && (maxCount == OPEN_INTERVAL || count <= maxCount);
	}

	@Override
	public Optional<Integer> getMinCount() {
		return Optional.of(minCount);
	}

	@Override
	public Optional<Integer> getMaxCount() {
		if (maxCount == OPEN_INTERVAL) {
			return Optional.empty();
		}
		return Optional.of(maxCount);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ConstantArgumentCount that = (ConstantArgumentCount) o;
		return minCount == that.minCount && maxCount == that.maxCount;
	}

	@Override
	public int hashCode() {
		return Objects.hash(minCount, maxCount);
	}
}
