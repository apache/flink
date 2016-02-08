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

package org.apache.flink.api.common.io;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.flink.annotation.Internal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;

/**
 * This is the default implementation of the {@link InputSplitAssigner} interface. The default input split assigner
 * simply returns all input splits of an input vertex in the order they were originally computed.
 */
@Internal
public class DefaultInputSplitAssigner implements InputSplitAssigner {

	/** The logging object used to report information and errors. */
	private static final Logger LOG = LoggerFactory.getLogger(DefaultInputSplitAssigner.class);

	/** The list of all splits */
	private final List<InputSplit> splits = new ArrayList<InputSplit>();


	public DefaultInputSplitAssigner(InputSplit[] splits) {
		Collections.addAll(this.splits, splits);
	}
	
	public DefaultInputSplitAssigner(Collection<? extends InputSplit> splits) {
		this.splits.addAll(splits);
	}
	
	
	@Override
	public InputSplit getNextInputSplit(String host, int taskId) {
		InputSplit next = null;
		
		// keep the synchronized part short
		synchronized (this.splits) {
			if (this.splits.size() > 0) {
				next = this.splits.remove(this.splits.size() - 1);
			}
		}
		
		if (LOG.isDebugEnabled()) {
			if (next == null) {
				LOG.debug("No more input splits available");
			} else {
				LOG.debug("Assigning split " + next + " to " + host);
			}
		}
		return next;
	}
}
