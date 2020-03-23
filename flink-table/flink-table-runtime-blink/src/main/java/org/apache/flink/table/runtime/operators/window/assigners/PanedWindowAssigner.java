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

package org.apache.flink.table.runtime.operators.window.assigners;

import org.apache.flink.table.runtime.operators.window.Window;

/**
 * A {@code WindowAssigner} that window can be split into panes.
 *
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
public abstract class PanedWindowAssigner<W extends Window> extends WindowAssigner<W> {

	private static final long serialVersionUID = 1L;

	/**
	 * Given the timestamp and element, returns the pane into which it should be placed.
	 * @param element The element to which windows should be assigned.
	 * @param timestamp The timestamp of the element when {@link #isEventTime()} returns true,
	 *                  or the current system time when {@link #isEventTime()} returns false.
	 */
	public abstract W assignPane(Object element, long timestamp);

	/**
	 * Splits the given window into panes collection.
	 * @param window the window to be split.
	 * @return the panes iterable
	 */
	public abstract Iterable<W> splitIntoPanes(W window);

	/**
	 * Gets the last window which the pane belongs to.
	 */
	public abstract W getLastWindow(W pane);
}
