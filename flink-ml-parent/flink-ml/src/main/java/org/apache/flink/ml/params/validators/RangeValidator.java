/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.params.validators;

import org.apache.flink.ml.api.misc.param.ParamValidator;

/**
 * Range Validtor.
 */
public class RangeValidator<T extends Comparable <T>> implements ParamValidator <T> {
	T minVal;
	T maxVal;
	boolean leftInclusive = true;
	boolean rightInclusive = true;

	public RangeValidator(T minVal, T maxVal) {
		this.minVal = minVal;
		this.maxVal = maxVal;
	}

	public RangeValidator <T> withLeftInclusive(boolean tag) {
		this.leftInclusive = tag;
		return this;
	}

	public RangeValidator <T> withRightInclusive(boolean tag) {
		this.rightInclusive = tag;
		return this;
	}

	public void setLeftInclusive(boolean leftInclusive) {
		this.leftInclusive = leftInclusive;
	}

	public void setRightInclusive(boolean rightInclusive) {
		this.rightInclusive = rightInclusive;
	}

	@Override
	public boolean validate(T v) {
		if (leftInclusive) {
			if (!(minVal.compareTo(v) <= 0)) {
				return false;
			}
		} else {
			if (!(minVal.compareTo(v) < 0)) {
				return false;
			}
		}
		if (rightInclusive) {
			if (!(maxVal.compareTo(v) >= 0)) {
				return false;
			}
		} else {
			if (!(maxVal.compareTo(v) > 0)) {
				return false;
			}
		}
		return true;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("in ");
		sb.append(leftInclusive ? "[" : "(");
		sb.append(minVal);
		sb.append(", ");
		sb.append(rightInclusive ? "]" : ")");
		return sb.toString();
	}
}
