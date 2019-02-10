/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.windowing.windows.Window;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@code SliceAssigner} assigns zero or one unique {@link Window Windows} to an element.
 *
 * <p>It specifies the type of window that are guaranteed to be non-overlapping.
 *
 * @param <T> The type of elements that this SliceAssigner can assign windows to.
 * @param <W> The type of {@code Window} that this assigner assigns.
 */
@PublicEvolving
public abstract class SliceAssigner<T, W extends Window> extends WindowAssigner<T, W> {
	private static final long serialVersionUID = 1L;

	/**
	 * Returns zero or one unique window that should be assigned to the element.
	 *
	 * @param element The element to which windows should be assigned.
	 * @param timestamp The timestamp of the element.
	 * @param context The {@link WindowAssignerContext} in which the assigner operates.
	 */
	public abstract W assignSlice(T element, long timestamp, WindowAssignerContext context);

	@Override
	public Collection<W> assignWindows(T element, long timestamp, WindowAssignerContext context) {
		return Collections.singleton(assignSlice(element, timestamp, context));
	}

	public abstract TypeInformation<W> getWindowType();
}
