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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.concurrent.BiFunction;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.util.ExceptionUtils;

import java.util.List;

/**
 * Utilities for dealing with the execution graphs and scheduling.
 */
public class ExecutionGraphUtils {

	/**
	 * Releases the slot represented by the given future. If the future is complete, the
	 * slot is immediately released. Otherwise, the slot is released as soon as the future
	 * is completed.
	 * 
	 * <p>Note that releasing the slot means cancelling any task execution currently
	 * associated with that slot.
	 * 
	 * @param slotFuture The future for the slot to release.
	 */
	public static void releaseSlotFuture(Future<SimpleSlot> slotFuture) {
		slotFuture.handle(ReleaseSlotFunction.INSTANCE);
	}

	/**
	 * Releases the all the slots in the list of arrays of {@code ExecutionAndSlot}.
	 * For each future in that collection holds: If the future is complete, its slot is
	 * immediately released. Otherwise, the slot is released as soon as the future
	 * is completed.
	 * 
	 * <p>This methods never throws any exceptions (except for fatal exceptions) and continues
	 * to release the remaining slots if one slot release failed.
	 *
	 * <p>Note that releasing the slot means cancelling any task execution currently
	 * associated with that slot.
	 * 
	 * @param resources The collection of ExecutionAndSlot whose slots should be released.
	 */
	public static void releaseAllSlotsSilently(List<ExecutionAndSlot[]> resources) {
		try {
			for (ExecutionAndSlot[] jobVertexResources : resources) {
				if (jobVertexResources != null) {
					for (ExecutionAndSlot execAndSlot : jobVertexResources) {
						if (execAndSlot != null) {
							try {
								releaseSlotFuture(execAndSlot.slotFuture);
							}
							catch (Throwable t) {
								ExceptionUtils.rethrowIfFatalError(t);
							}
						}
					}
				}
			}
		}
		catch (Throwable t) {
			ExceptionUtils.rethrowIfFatalError(t);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A function to be applied into a future, releasing the slot immediately upon completion.
	 * Completion here refers to both the successful and exceptional completion.
	 */
	private static final class ReleaseSlotFunction implements BiFunction<SimpleSlot, Throwable, Void> {

		static final ReleaseSlotFunction INSTANCE = new ReleaseSlotFunction();

		@Override
		public Void apply(SimpleSlot simpleSlot, Throwable throwable) {
			if (simpleSlot != null) {
				simpleSlot.releaseSlot();
			}
			return null;
		}
	}

	// ------------------------------------------------------------------------

	/** Utility class is not meant to be instantiated */
	private ExecutionGraphUtils() {}
}
