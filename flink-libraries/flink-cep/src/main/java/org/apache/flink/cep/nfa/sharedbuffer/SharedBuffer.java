/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOVICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Vhe ASF licenses this file
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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.DeweyNumber;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.flink.cep.nfa.compiler.NFAStateNameHandler.getOriginalNameFromInternal;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A shared buffer implementation which stores values under according state. Additionally, the values can be
 * versioned such that it is possible to retrieve their predecessor element in the buffer.
 *
 * <p>The idea of the implementation is to have a buffer for incoming events with unique ids assigned to them. This way
 * we do not need to deserialize events during processing and we store only one copy of the event.
 *
 * <p>The entries in {@link SharedBuffer} are {@link SharedBufferNode}. The shared buffer node allows to store
 * relations between different entries. A dewey versioning scheme allows to discriminate between
 * different relations (e.g. preceding element).
 *
 * <p>The implementation is strongly based on the paper "Efficient Pattern Matching over Event Streams".
 *
 * @param <V> Type of the values
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 * https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 */
public class SharedBuffer<V> {

	private static final String entriesStateName = "sharedBuffer-entries";
	private static final String eventsStateName = "sharedBuffer-events";
	private static final String eventsCountStateName = "sharedBuffer-events-count";

	/** The buffer holding the unique events seen so far. */
	private MapState<Long, EventWrapper<V>> eventsBuffer;

	/** The number of events seen so far in the stream? or the ones currently in the buffer? */
	private ValueState<Long> eventsCount;
	private MapState<NodeId, SharedBufferNode> pages;

	public SharedBuffer(KeyedStateStore stateStore, TypeSerializer<V> valueSerializer) {
		this.eventsBuffer = stateStore.getMapState(
				new MapStateDescriptor<>(
						eventsStateName,
						LongSerializer.INSTANCE,
						new EventWrapper.EventWrapperTypeSerializer<>(valueSerializer)));

		this.pages = stateStore.getMapState(
				new MapStateDescriptor<>(
						entriesStateName,
						NodeId.NodeIdSerializer.INSTANCE,
						new SharedBufferNode.SharedBufferNodeSerializer()));

		this.eventsCount = stateStore.getState(
				new ValueStateDescriptor<>(
						eventsCountStateName,
						LongSerializer.INSTANCE));
	}

	/**
	 * Adds another unique event to the shared buffer and assigns a unique id for it. It automatically creates a
	 * lock on this event, so it won't be removed during processing of that event. Therefore the lock should be removed
	 * after processing all {@link org.apache.flink.cep.nfa.ComputationState}s
	 *
	 * <p><b>NOTE:</b>Should be called only once for each unique event!
	 *
	 * @param value event to be registered
	 * @return unique id of that event that should be used when putting entries to the buffer.
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public long registerEvent(V value) throws Exception {
		Long eventId = eventsCount.value();
		if (eventId == null) {
			eventId = 0L;
		}

		eventsBuffer.put(eventId, new EventWrapper<>(value, 1));
		eventsCount.update(eventId + 1L);
		return eventId;
	}

	/**
	 * Initializes underlying state with given map of events and entries. Should be used only in case of migration from
	 * old state.
	 *
	 * @param events  map of events with assigned unique ids
	 * @param entries map of SharedBufferNodes
	 * @throws Exception Thrown if the system cannot access the state.
	 * @deprecated Only for state migration!
	 */
	@Deprecated
	public void init(Map<Long, EventWrapper<V>> events, Map<NodeId, SharedBufferNode> entries) throws Exception {
		eventsBuffer.putAll(events);
		pages.putAll(entries);
		eventsCount.update(events.keySet().stream().mapToLong(id -> id).max().orElse(-1) + 1);
	}

	/**
	 * Stores given value (value + timestamp) under the given state. It assigns no preceding element
	 * relation to the entry.
	 *
	 * @param stateName name of the state that the event should be assigned to
	 * @param eventId   unique id of event assigned by this SharedBuffer
	 * @param timestamp Timestamp of the current value
	 * @param version   Version of the previous relation
	 * @return assigned id of this entry
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public NodeId put(
			final String stateName,
			final long eventId,
			final long timestamp,
			final DeweyNumber version) throws Exception {

		return put(stateName, eventId, timestamp, null, version);
	}

	/**
	 * Stores given value (value + timestamp) under the given state. It assigns a preceding element
	 * relation to the previous entry.
	 *
	 * @param stateName     name of the state that the event should be assigned to
	 * @param eventId       unique id of event assigned by this SharedBuffer
	 * @param timestamp     Timestamp of the current value
	 * @param previousNodeId id of previous entry
	 * @param version       Version of the previous relation
	 * @return assigned id of this element
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public NodeId put(
			final String stateName,
			final long eventId,
			final long timestamp,
			@Nullable final NodeId previousNodeId,
			final DeweyNumber version) throws Exception {

		if (previousNodeId != null) {
			lockNode(previousNodeId);
		}

		NodeId currentNodeId = new NodeId(eventId, timestamp, getOriginalNameFromInternal(stateName));
		SharedBufferNode currentNode = pages.get(currentNodeId);
		if (currentNode == null) {
			currentNode = new SharedBufferNode(0);
			lockEvent(eventId);
		}

		currentNode.addEdge(new SharedBufferEdge(
			previousNodeId,
			version));
		pages.put(currentNodeId, currentNode);

		return currentNodeId;
	}

	/**
	 * Checks if there is no elements in the buffer.
	 *
	 * @return true if there is no elements in the buffer
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public boolean isEmpty() throws Exception {
		return Iterables.isEmpty(eventsBuffer.keys());
	}

	/**
	 * Returns all elements from the previous relation starting at the given entry.
	 *
	 * @param nodeId  id of the starting entry
	 * @param version Version of the previous relation which shall be extracted
	 * @return Collection of previous relations starting with the given value
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public List<Map<String, List<V>>> extractPatterns(
			final NodeId nodeId,
			final DeweyNumber version) throws Exception {

		List<Map<String, List<V>>> result = new ArrayList<>();

		// stack to remember the current extraction states
		Stack<ExtractionState> extractionStates = new Stack<>();

		// get the starting shared buffer entry for the previous relation
		SharedBufferNode entry = pages.get(nodeId);

		if (entry != null) {
			extractionStates.add(new ExtractionState(Tuple2.of(nodeId, entry), version, new Stack<>()));

			// use a depth first search to reconstruct the previous relations
			while (!extractionStates.isEmpty()) {
				final ExtractionState extractionState = extractionStates.pop();
				// current path of the depth first search
				final Stack<Tuple2<NodeId, SharedBufferNode>> currentPath = extractionState.getPath();
				final Tuple2<NodeId, SharedBufferNode> currentEntry = extractionState.getEntry();

				// termination criterion
				if (currentEntry == null) {
					final Map<String, List<V>> completePath = new LinkedHashMap<>();

					while (!currentPath.isEmpty()) {
						final NodeId currentPathEntry = currentPath.pop().f0;

						String page = currentPathEntry.getPageName();
						List<V> values = completePath
							.computeIfAbsent(page, k -> new ArrayList<>());
						values.add(eventsBuffer.get(currentPathEntry.getEventId()).getEvent());
					}
					result.add(completePath);
				} else {

					// append state to the path
					currentPath.push(currentEntry);

					boolean firstMatch = true;
					for (SharedBufferEdge edge : currentEntry.f1.getEdges()) {
						// we can only proceed if the current version is compatible to the version
						// of this previous relation
						final DeweyNumber currentVersion = extractionState.getVersion();
						if (currentVersion.isCompatibleWith(edge.getDeweyNumber())) {
							final NodeId target = edge.getTarget();
							Stack<Tuple2<NodeId, SharedBufferNode>> newPath;

							if (firstMatch) {
								// for the first match we don't have to copy the current path
								newPath = currentPath;
								firstMatch = false;
							} else {
								newPath = new Stack<>();
								newPath.addAll(currentPath);
							}

							extractionStates.push(new ExtractionState(
								target != null ? Tuple2.of(target, pages.get(target)) : null,
								edge.getDeweyNumber(),
								newPath));
						}
					}
				}

			}
		}
		return result;
	}

	/**
	 * Increases the reference counter for the given entry so that it is not
	 * accidentally removed.
	 *
	 * @param node id of the entry
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public void lockNode(final NodeId node) throws Exception {
		SharedBufferNode sharedBufferNode = pages.get(node);
		if (sharedBufferNode != null) {
			sharedBufferNode.getLock().lock();
			pages.put(node, sharedBufferNode);
		}
	}

	/**
	 * Decreases the reference counter for the given entry so that it can be
	 * removed once the reference counter reaches 0.
	 *
	 * @param node id of the entry
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public void releaseNode(final NodeId node) throws Exception {
		SharedBufferNode sharedBufferNode = pages.get(node);
		if (sharedBufferNode != null) {
			if (sharedBufferNode.getLock().release()) {
				removeNode(node, sharedBufferNode);
			} else {
				pages.put(node, sharedBufferNode);
			}
		}
	}

	private void removeNode(NodeId node, SharedBufferNode sharedBufferNode) throws Exception {
		pages.remove(node);
		long eventId = node.getEventId();
		releaseEvent(eventId);

		for (SharedBufferEdge sharedBufferEdge : sharedBufferNode.getEdges()) {
			releaseNode(sharedBufferEdge.getTarget());
		}
	}

	private void lockEvent(long eventId) throws Exception {
		EventWrapper<V> eventWrapper = eventsBuffer.get(eventId);
		checkState(
			eventWrapper != null,
			"Referring to non existent event with id %s",
			eventId);
		eventWrapper.getLock().lock();
		eventsBuffer.put(eventId, eventWrapper);
	}

	/**
	 * Decreases the reference counter for the given event so that it can be
	 * removed once the reference counter reaches 0.
	 *
	 * @param eventId id of the event
	 * @throws Exception Thrown if the system cannot access the state.
	 */
	public void releaseEvent(long eventId) throws Exception {
		EventWrapper<V> eventWrapper = eventsBuffer.get(eventId);
		if (eventWrapper != null) {
			if (eventWrapper.getLock().release()) {
				eventsBuffer.remove(eventId);
			} else {
				eventsBuffer.put(eventId, eventWrapper);
			}
		}
	}

	/**
	 * Helper class to store the extraction state while extracting a sequence of values following
	 * the versioned entry edges.
	 */
	private static class ExtractionState {

		private final Tuple2<NodeId, SharedBufferNode> entry;
		private final DeweyNumber version;
		private final Stack<Tuple2<NodeId, SharedBufferNode>> path;

		ExtractionState(
				final Tuple2<NodeId, SharedBufferNode> entry,
				final DeweyNumber version,
				final Stack<Tuple2<NodeId, SharedBufferNode>> path) {
			this.entry = entry;
			this.version = version;
			this.path = path;
		}

		public Tuple2<NodeId, SharedBufferNode> getEntry() {
			return entry;
		}

		public Stack<Tuple2<NodeId, SharedBufferNode>> getPath() {
			return path;
		}

		public DeweyNumber getVersion() {
			return version;
		}

		@Override
		public String toString() {
			return "ExtractionState(" + entry + ", " + version + ", [" +
				StringUtils.join(path, ", ") + "])";
		}
	}

}
