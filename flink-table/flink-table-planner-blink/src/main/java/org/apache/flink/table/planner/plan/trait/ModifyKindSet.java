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

package org.apache.flink.table.planner.plan.trait;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The set of modify operations contained in a changelog.
 *
 * @see ModifyKind
 */
public class ModifyKindSet {

	/**
	 * Insert-only modify kind set.
	 */
	public static final ModifyKindSet INSERT_ONLY = ModifyKindSet.newBuilder()
		.addContainedKind(ModifyKind.INSERT)
		.build();

	/**
	 * A modify kind set contains all change operations.
	 */
	public static final ModifyKindSet ALL_CHANGES = ModifyKindSet.newBuilder()
		.addContainedKind(ModifyKind.INSERT)
		.addContainedKind(ModifyKind.UPDATE)
		.addContainedKind(ModifyKind.DELETE)
		.build();

	private final Set<ModifyKind> kinds;

	private ModifyKindSet(Set<ModifyKind> kinds) {
		this.kinds = Collections.unmodifiableSet(kinds);
	}

	public Set<ModifyKind> getContainedKinds() {
		return kinds;
	}

	public boolean contains(ModifyKind kind) {
		return kinds.contains(kind);
	}

	public boolean containsOnly(ModifyKind kind) {
		return kinds.size() == 1 && kinds.contains(kind);
	}

	public boolean isInsertOnly() {
		return containsOnly(ModifyKind.INSERT);
	}

	public int size() {
		return kinds.size();
	}

	public boolean isEmpty() {
		return kinds.isEmpty();
	}

	/**
	 * Returns a new set of ModifyKind which is this set minus the other set,
	 * i.e. {@code this.kinds - that.kinds}. For example:
	 * [I,U,D] minus [I] = [U,D]
	 * [I,U] minus [U,D] = [I]
	 * [I,U,D] minus [I,U,D] = []
	 */
	public ModifyKindSet minus(ModifyKindSet other) {
		Set<ModifyKind> result = EnumSet.noneOf(ModifyKind.class);
		result.addAll(this.kinds);
		result.removeAll(other.kinds);
		return new ModifyKindSet(result);
	}

	/**
	 * Returns a new ModifyKindSet with all kinds set in both this set and in another set.
	 */
	public ModifyKindSet intersect(ModifyKindSet other) {
		Builder builder = new Builder();
		for (ModifyKind kind : other.getContainedKinds()) {
			if (this.contains(kind)) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}

	/**
	 * Returns a new ModifyKindSet with the union of the other ModifyKindSet.
	 */
	public ModifyKindSet union(ModifyKindSet other) {
		return union(this, other);
	}

	/**
	 * Returns the default {@link ChangelogMode} from this {@link ModifyKindSet}.
	 */
	public ChangelogMode toChangelogMode() {
		ChangelogMode.Builder builder = ChangelogMode.newBuilder();
		if (this.contains(ModifyKind.INSERT)) {
			builder.addContainedKind(RowKind.INSERT);
		}
		if (this.contains(ModifyKind.UPDATE)) {
			builder.addContainedKind(RowKind.UPDATE_BEFORE);
			builder.addContainedKind(RowKind.UPDATE_AFTER);
		}
		if (this.contains(ModifyKind.DELETE)) {
			builder.addContainedKind(RowKind.DELETE);
		}
		return builder.build();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ModifyKindSet that = (ModifyKindSet) o;
		return Objects.equals(kinds, that.kinds);
	}

	@Override
	public int hashCode() {
		return Objects.hash(kinds);
	}

	@Override
	public String toString() {
		if (kinds.isEmpty()) {
			return "NONE";
		}
		List<String> modifyKinds = new ArrayList<>();
		if (contains(ModifyKind.INSERT)) {
			modifyKinds.add("I");
		}
		if (contains(ModifyKind.UPDATE)) {
			modifyKinds.add("U");
		}
		if (contains(ModifyKind.DELETE)) {
			modifyKinds.add("D");
		}
		return String.join(",", modifyKinds);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the union of a number of ModifyKindSets.
	 */
	public static ModifyKindSet union(ModifyKindSet... modifyKindSets) {
		Builder builder = newBuilder();
		for (ModifyKindSet set : modifyKindSets) {
			for (ModifyKind kind : set.getContainedKinds()) {
				builder.addContainedKind(kind);
			}
		}
		return builder.build();
	}

	/**
	 * Builder for configuring and creating instances of {@link ModifyKindSet}.
	 */
	public static Builder newBuilder() {
		return new Builder();
	}

	/**
	 * Builder for configuring and creating instances of {@link ModifyKindSet}.
	 */
	public static class Builder {

		private final Set<ModifyKind> kinds = EnumSet.noneOf(ModifyKind.class);

		private Builder() {
			// default constructor to allow a fluent definition
		}

		public Builder addContainedKind(ModifyKind kind) {
			this.kinds.add(kind);
			return this;
		}

		public ModifyKindSet build() {
			return new ModifyKindSet(kinds);
		}
	}
}
