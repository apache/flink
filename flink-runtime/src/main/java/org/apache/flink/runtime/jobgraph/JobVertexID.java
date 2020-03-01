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

import org.apache.flink.runtime.topology.VertexID;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

/**
 * A class for statistically unique job vertex IDs.
 */
public class JobVertexID extends AbstractID implements VertexID {

	private static final long serialVersionUID = 1L;

	public JobVertexID() {
		super();
	}

	public JobVertexID(byte[] bytes) {
		super(bytes);
	}

	public JobVertexID(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}

	public static JobVertexID fromHexString(String hexString) {
		return new JobVertexID(StringUtils.hexStringToByte(hexString));
	}
}
