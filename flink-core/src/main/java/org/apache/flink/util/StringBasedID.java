/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import java.io.Serializable;

/**
 * Base class for typed IDs that are internally represented by a string. This class is not intended
 * for direct use, but should be subclassed for type-safety.
 */
public class StringBasedID implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Uses a String as internal representation.
	 */
	private final String keyString;

	/**
	 * Protected constructor to enforce that subclassing.
	 */
	protected StringBasedID(String keyString) {
		this.keyString = Preconditions.checkNotNull(keyString);
	}

	public String getKeyString() {
		return keyString;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		StringBasedID that = (StringBasedID) o;
		return keyString.equals(that.keyString);
	}

	@Override
	public int hashCode() {
		return keyString.hashCode();
	}

	@Override
	public String toString() {
		return keyString;
	}
}
