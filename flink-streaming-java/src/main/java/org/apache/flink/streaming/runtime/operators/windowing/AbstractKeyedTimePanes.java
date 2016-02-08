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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;

/**
 * Base class for a multiple key/value maps organized in panes.
 */
@Internal
public abstract class AbstractKeyedTimePanes<Type, Key, Aggregate, Result> {
	
	private static final int BEGIN_OF_STATE_MAGIC_NUMBER = 0x0FF1CE42;

	private static final int BEGIN_OF_PANE_MAGIC_NUMBER = 0xBADF00D5;
	
	/** The latest time pane */
	protected KeyMap<Key, Aggregate> latestPane = new KeyMap<>();

	/** The previous time panes, ordered by time (early to late) */
	protected final ArrayDeque<KeyMap<Key, Aggregate>> previousPanes = new ArrayDeque<>();

	// ------------------------------------------------------------------------

	public abstract void addElementToLatestPane(Type element) throws Exception;

	public abstract void evaluateWindow(Collector<Result> out, TimeWindow window, AbstractStreamOperator<Result> operator) throws Exception;
	
	
	public void dispose() {
		// since all is heap data, there is no need to clean up anything
		latestPane = null;
		previousPanes.clear();
	}
	
	public int getNumPanes() {
		return previousPanes.size() + 1;
	}
	
	
	public void slidePanes(int panesToKeep) {
		if (panesToKeep > 1) {
			// the current pane becomes the latest previous pane
			previousPanes.addLast(latestPane);

			// truncate the history
			while (previousPanes.size() >= panesToKeep) {
				previousPanes.removeFirst();
			}
		}

		// we need a new latest pane
		latestPane = new KeyMap<>();
	}
	
	public void truncatePanes(int numToRetain) {
		while (previousPanes.size() >= numToRetain) {
			previousPanes.removeFirst();
		}
	}
	
	protected void traverseAllPanes(KeyMap.TraversalEvaluator<Key, Aggregate> traversal, long traversalPass) throws Exception{
		// gather all panes in an array (faster iterations)
		@SuppressWarnings({"unchecked", "rawtypes"})
		KeyMap<Key, Aggregate>[] panes = previousPanes.toArray(new KeyMap[previousPanes.size() + 1]);
		panes[panes.length - 1] = latestPane;

		// let the maps make a coordinated traversal and evaluate the window function per contained key
		KeyMap.traverseMaps(panes, traversal, traversalPass);
	}
	
	// ------------------------------------------------------------------------
	//  Serialization and de-serialization
	// ------------------------------------------------------------------------

	public void writeToOutput(
			final DataOutputView output,
			final TypeSerializer<Key> keySerializer,
			final TypeSerializer<Aggregate> aggSerializer) throws IOException
	{
		output.writeInt(BEGIN_OF_STATE_MAGIC_NUMBER);
		
		int numPanes = getNumPanes();
		output.writeInt(numPanes);
		
		// write from the past
		Iterator<KeyMap<Key, Aggregate>> previous = previousPanes.iterator();
		for (int paneNum = 0; paneNum < numPanes; paneNum++) {
			output.writeInt(BEGIN_OF_PANE_MAGIC_NUMBER);
			KeyMap<Key, Aggregate> pane = (paneNum == numPanes - 1) ? latestPane : previous.next();
			
			output.writeInt(pane.size());
			for (KeyMap.Entry<Key, Aggregate> entry : pane) {
				keySerializer.serialize(entry.getKey(), output);
				aggSerializer.serialize(entry.getValue(), output);
			}
		}
	}
	
	public void readFromInput(
			final DataInputView input,
			final TypeSerializer<Key> keySerializer,
			final TypeSerializer<Aggregate> aggSerializer) throws IOException
	{
		validateMagicNumber(BEGIN_OF_STATE_MAGIC_NUMBER, input.readInt());
		int numPanes = input.readInt();
		
		// read from the past towards the presence
		while (numPanes > 0) {
			validateMagicNumber(BEGIN_OF_PANE_MAGIC_NUMBER, input.readInt());
			KeyMap<Key, Aggregate> pane = (numPanes == 1) ? latestPane : new KeyMap<Key, Aggregate>();
			
			final int numElementsInPane = input.readInt();
			for (int i = numElementsInPane - 1; i >= 0; i--) {
				Key k = keySerializer.deserialize(input);
				Aggregate a = aggSerializer.deserialize(input);
				pane.put(k, a);
			}
			
			if (numPanes > 1) {
				previousPanes.addLast(pane);
			}
			numPanes--;
		}
	}
	
	private static void validateMagicNumber(int expected, int found) throws IOException {
		if (expected != found) {
			throw new IOException("Corrupt state stream - wrong magic number. " +
				"Expected '" + Integer.toHexString(expected) +
				"', found '" + Integer.toHexString(found) + '\'');
		}
	}
}
