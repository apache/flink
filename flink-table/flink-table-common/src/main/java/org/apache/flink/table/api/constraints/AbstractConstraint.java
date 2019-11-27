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

package org.apache.flink.table.api.constraints;

import org.apache.flink.annotation.Internal;

import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for {@link Constraint constraints}.
 */
@Internal
abstract class AbstractConstraint implements Constraint {
	private final String name;
	private final boolean enforced;

	AbstractConstraint(String name, boolean enforced) {
		this.name = checkNotNull(name);
		this.enforced = enforced;
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public boolean isEnforced() {
		return enforced;
	}

	/**
	 * Returns the in brackets part of a constraint summary.
	 */
	abstract String constraintDefinition();

	/**
	 * Returns constraint's summary. All constraints summary will be formatted as
	 * <pre>
	 * CONSTRAINT [constraint-name] [constraint-type] ([constraint-definition])
	 *
	 * E.g CONSTRAINT pk PRIMARY KEY (f0, f1)
	 * </pre>
	 */
	@Override
	public final String asSummaryString() {
		final String typeString;
		switch (getType()) {
			case PRIMARY_KEY:
				typeString = "PRIMARY KEY";
				break;
			case UNIQUE_KEY:
				typeString = "UNIQUE";
				break;
			default:
				throw new IllegalStateException("Unknown key type: " + getType());
		}

		return String.format("CONSTRAINT %s %s (%s)", getName(), typeString, constraintDefinition());
	}

	@Override
	public String toString() {
		return asSummaryString();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		AbstractConstraint that = (AbstractConstraint) o;
		return enforced == that.enforced &&
			Objects.equals(name, that.name);
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, enforced);
	}
}
