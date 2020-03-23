/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.function.FunctionWithException;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A class that is responsible for tracking the past split assignments made by
 * {@link SplitEnumerator}.
 */
@Internal
public class SplitAssignmentTracker<SplitT extends SourceSplit> {
	// All the split assignments since the last successful checkpoint.
	// Maintaining this allow the subtasks to fail over independently.
	// The mapping is [CheckpointId -> [SubtaskId -> LinkedHashSet[SourceSplits]]].
	private final SortedMap<Long, Map<Integer, LinkedHashSet<SplitT>>> assignmentsByCheckpointId;
	// The split assignments since the last checkpoint attempt.
	// The mapping is [SubtaskId -> LinkedHashSet[SourceSplits]].
	private Map<Integer, LinkedHashSet<SplitT>> uncheckpointedAssignments;
	// The current split assignments. This is an aggregated view of all the historical assignments.
	private final Map<Integer, LinkedHashSet<SplitT>> currentAssignments;

	public SplitAssignmentTracker() {
		this.assignmentsByCheckpointId = new TreeMap<>();
		this.uncheckpointedAssignments = new HashMap<>();
		this.currentAssignments = new HashMap<>();
	}

	/**
	 * Take a snapshot of the uncheckpointed split assignments.
	 *
	 * @param checkpointId the id of the ongoing checkpoint
	 */
	public void snapshotState(
			long checkpointId,
			SimpleVersionedSerializer<SplitT> splitSerializer,
			ObjectOutput out) throws Exception {
		// Write the split serializer version.
		out.writeInt(splitSerializer.getVersion());

		// Include the uncheckpointed assignments to the snapshot.
		assignmentsByCheckpointId.put(checkpointId, uncheckpointedAssignments);
		uncheckpointedAssignments = new HashMap<>();
		Map<Long, Map<Integer, LinkedHashSet<byte[]>>> serializedState = convertAssignmentsByCheckpoints(
				assignmentsByCheckpointId, splitSerializer::serialize);
		out.writeObject(serializedState);

		// Write the current assignment to the snapshot.
		out.writeObject(convertAssignment(currentAssignments, splitSerializer::serialize));
	}

	/**
	 * Restore the state of the SplitAssignmentTracker.
	 *
	 * @param splitSerializer The serializer of the splits.
	 * @param in The ObjectInput that contains the state of the SplitAssignmentTracker.
	 * @throws Exception when the state deserialization fails.
	 */
	@SuppressWarnings("unchecked")
	public void restoreState(SimpleVersionedSerializer<SplitT> splitSerializer, ObjectInput in) throws Exception {
		// Read the version.
		final int version = in.readInt();

		// Read the split assignments by checkpoint id.
		Map<Long, Map<Integer, LinkedHashSet<byte[]>>> serializedState =
				(Map<Long, Map<Integer, LinkedHashSet<byte[]>>>) in.readObject();
		Map<Long, Map<Integer, LinkedHashSet<SplitT>>> deserializedState = convertAssignmentsByCheckpoints(
				serializedState,
				(byte[] splitBytes) -> splitSerializer.deserialize(version, splitBytes));
		assignmentsByCheckpointId.putAll(deserializedState);

		// Read the current assignments.
		Map<Integer, LinkedHashSet<byte[]>> serializedAssignments = (Map<Integer, LinkedHashSet<byte[]>>) in.readObject();
		Map<Integer, LinkedHashSet<SplitT>> assignments = convertAssignment(
				serializedAssignments,
				(byte[] serializedSplit) -> splitSerializer.deserialize(version, serializedSplit));
		currentAssignments.putAll(assignments);
	}

	/**
	 * when a checkpoint has been successfully made, this method is invoked to clean up the assignment
	 * history before this successful checkpoint.
	 *
	 * @param checkpointId the id of the successful checkpoint.
	 */
	public void onCheckpointComplete(long checkpointId) {
		assignmentsByCheckpointId.entrySet().removeIf(entry -> entry.getKey() <= checkpointId);
	}

	/**
	 * Get the current split assignment.
	 *
	 * @return the current split assignment.
	 */
	Map<Integer, LinkedHashSet<SplitT>> currentSplitsAssignment() {
		return Collections.unmodifiableMap(currentAssignments);
	}

	/**
	 * Record a new split assignment.
	 *
	 * @param splitsAssignment the new split assignment.
	 */
	public void recordSplitAssignment(SplitsAssignment<SplitT> splitsAssignment) {
		addSplitAssignment(splitsAssignment, currentAssignments);
		addSplitAssignment(splitsAssignment, uncheckpointedAssignments);
	}

	/**
	 * This method is invoked when a source reader fails over. In this case, the source reader will
	 * restore its split assignment to the last successful checkpoint. Any split assignment to that
	 * source reader after the last successful checkpoint will be lost on the source reader side as
	 * if those splits were never assigned. To handle this case, the coordinator needs to find those
	 * splits and return them back to the SplitEnumerator for re-assignment.
	 *
	 * @param failedSubtaskId the failed subtask id.
	 * @return A list of splits that needs to be added back to the {@link SplitEnumerator}.
	 */
	public List<SplitT> getAndRemoveUncheckpointedAssignment(int failedSubtaskId) {
		List<SplitT> toPutBack = splitsToAddBack(failedSubtaskId);
		LinkedHashSet<SplitT> assignment = currentAssignments.get(failedSubtaskId);
		assignment.removeAll(toPutBack);
		if (assignment.isEmpty()) {
			currentAssignments.remove(failedSubtaskId);
		}
		return toPutBack;
	}

	// ------------- Methods visible for testing ----------------

	@VisibleForTesting
	SortedMap<Long, Map<Integer, LinkedHashSet<SplitT>>> assignmentsByCheckpointId() {
		return assignmentsByCheckpointId;
	}

	@VisibleForTesting
	Map<Integer, LinkedHashSet<SplitT>> assignmentsByCheckpointId(long checkpointId) {
		return assignmentsByCheckpointId.get(checkpointId);
	}

	@VisibleForTesting
	Map<Integer, LinkedHashSet<SplitT>> uncheckpointedAssignments() {
		return uncheckpointedAssignments;
	}

	// -------------- private helpers ---------------

	private List<SplitT> splitsToAddBack(int subtaskId) {
		List<SplitT> splits = new ArrayList<>();
		assignmentsByCheckpointId.values().forEach(assignments -> {
			removeFromAssignment(subtaskId, assignments, splits);
		});
		removeFromAssignment(subtaskId, uncheckpointedAssignments, splits);
		return splits;
	}

	private void removeFromAssignment(
			int subtaskId,
			Map<Integer, LinkedHashSet<SplitT>> assignments,
			List<SplitT> toPutBack) {
		Set<SplitT> splitForSubtask = assignments.remove(subtaskId);
		if (splitForSubtask != null) {
			toPutBack.addAll(splitForSubtask);
		}
	}

	private void addSplitAssignment(
			SplitsAssignment<SplitT> additionalAssignment,
			Map<Integer, LinkedHashSet<SplitT>> assignments) {
		additionalAssignment.assignment().forEach((id, splits) ->
				assignments.computeIfAbsent(id, ignored -> new LinkedHashSet<>()).addAll(splits));
	}

	// -------------- private util methods -----------------

	private static <S, T> Map<Long, Map<Integer, LinkedHashSet<T>>> convertAssignmentsByCheckpoints(
			Map<Long, Map<Integer, LinkedHashSet<S>>> uncheckpiontedAssignments,
			FunctionWithException<S, T, Exception> converter) throws Exception {
		Map<Long, Map<Integer, LinkedHashSet<T>>> targetSplitsContextCkpt = new HashMap<>(uncheckpiontedAssignments.size());
		for (Map.Entry<Long, Map<Integer, LinkedHashSet<S>>> ckptEntry : uncheckpiontedAssignments.entrySet()) {
			targetSplitsContextCkpt.put(
					ckptEntry.getKey(),
					convertAssignment(ckptEntry.getValue(), converter));
		}
		return targetSplitsContextCkpt;
	}

	private static <S, T> Map<Integer, LinkedHashSet<T>> convertAssignment(
			Map<Integer, LinkedHashSet<S>> assignment,
			FunctionWithException<S, T, Exception> converter) throws Exception {
		Map<Integer, LinkedHashSet<T>> convertedAssignments = new HashMap<>();
		for (Map.Entry<Integer, LinkedHashSet<S>> assignmentEntry : assignment.entrySet()) {
			LinkedHashSet<T> serializedSplits = convertedAssignments.compute(
					assignmentEntry.getKey(),
					(taskId, ignored) -> new LinkedHashSet<>(assignmentEntry.getValue().size()));
			for (S source : assignmentEntry.getValue()) {
				serializedSplits.add(converter.apply(source));
			}
		}
		return convertedAssignments;
	}
}
