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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.runtime.topology.ResultID;
import org.apache.flink.util.AbstractID;

import java.util.UUID;

/**
 * Id identifying {@link IntermediateDataSet}.
 */
public class IntermediateDataSetID extends AbstractID implements ResultID {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates an new random intermediate data set ID.
	 */
	public IntermediateDataSetID() {
		super();
	}

	/**
	 * Creates a new intermediate data set ID with the bytes of the given ID.
	 *
	 * @param from The ID to create this ID from.
	 */
	public IntermediateDataSetID(AbstractID from) {
		super(from);
	}

	/**
	 * Creates a new intermediate data set ID with the bytes of the given UUID.
	 *
	 * @param from The UUID to create this ID from.
	 */
	public IntermediateDataSetID(UUID from) {
		super(from.getLeastSignificantBits(), from.getMostSignificantBits());
	}
}
