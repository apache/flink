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

package org.apache.flink.runtime.taskexecutor;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A report about the current status of all slots of the TaskExecutor, describing
 * which slots are available and allocated, and what jobs (JobManagers) the allocated slots
 * have been allocated to.
 */
public class SlotReport implements Serializable, Iterable<SlotStatus> {

	private static final long serialVersionUID = -3150175198722481689L;

	/** The slots status of the TaskManager. */
	private final Collection<SlotStatus> slotsStatus;

	public SlotReport() {
		this(Collections.<SlotStatus>emptyList());
	}

	public SlotReport(SlotStatus slotStatus) {
		this(Collections.singletonList(slotStatus));
	}

	public SlotReport(final Collection<SlotStatus> slotsStatus) {
		this.slotsStatus = checkNotNull(slotsStatus);
	}

	public int getNumSlotStatus() {
		return slotsStatus.size();
	}

	@Override
	public Iterator<SlotStatus> iterator() {
		return slotsStatus.iterator();
	}

	@Override
	public String toString() {
		return "SlotReport{" +
			"slotsStatus=" + slotsStatus +
			'}';
	}
}
