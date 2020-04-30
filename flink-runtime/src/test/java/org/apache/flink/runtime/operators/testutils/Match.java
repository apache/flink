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

package org.apache.flink.runtime.operators.testutils;

/**
 * Utility class for keeping track of matches in join operator tests.
 *
 * @see MatchRemovingJoiner
 */
public class Match {
	private final String left;

	private final String right;

	public Match(String left, String right) {
		this.left = left;
		this.right = right;
	}

	@Override
	public boolean equals(Object obj) {
		Match o = (Match) obj;
		if (left == null && o.left == null && right.equals(o.right)) {
			return true;
		} else if (right == null && o.right == null && left.equals(o.left)) {
			return true;
		} else {
			return this.left.equals(o.left) && this.right.equals(o.right);
		}
	}

	@Override
	public int hashCode() {
		if (left == null) {
			return right.hashCode();
		} else if (right == null) {
			return left.hashCode();
		} else {
			return this.left.hashCode() ^ this.right.hashCode();
		}
	}

	@Override
	public String toString() {
		return left + ", " + right;
	}
}
