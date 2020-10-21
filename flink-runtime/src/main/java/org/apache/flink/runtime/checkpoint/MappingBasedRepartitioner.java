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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A repartitioner that assigns the same channel state to multiple subtasks according to some mapping.
 *
 * <p>The replicated data will then be filtered before processing the record.
 *
 * <p>Note that channel mappings are cached for the same parallelism changes.
 */
@NotThreadSafe
public class MappingBasedRepartitioner<T> implements OperatorStateRepartitioner<T> {
	private final Map<Integer, Set<Integer>> newToOldSubtasksMapping;

	public MappingBasedRepartitioner(Map<Integer, Set<Integer>> newToOldSubtasksMapping) {
		this.newToOldSubtasksMapping = newToOldSubtasksMapping;
	}

	private static <T> List<T> extractOldState(List<List<T>> previousParallelSubtaskStates, Set<Integer> oldIndexes) {
		switch (oldIndexes.size()) {
			case 0:
				return Collections.emptyList();
			case 1:
				return previousParallelSubtaskStates.get(Iterables.getOnlyElement(oldIndexes));
			default:
				return oldIndexes.stream()
					.flatMap(oldIndex -> previousParallelSubtaskStates.get(oldIndex).stream())
					.collect(Collectors.toList());
		}
	}

	@Override
	public List<List<T>> repartitionState(
			List<List<T>> previousParallelSubtaskStates,
			int oldParallelism,
			int newParallelism) {
		checkState(newParallelism == newToOldSubtasksMapping.size());

		List<List<T>> repartitioned = new ArrayList<>();
		for (int newIndex = 0; newIndex < newParallelism; newIndex++) {
			repartitioned.add(extractOldState(previousParallelSubtaskStates, newToOldSubtasksMapping.get(newIndex)));
		}
		return repartitioned;
	}
}
