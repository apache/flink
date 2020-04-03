/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.rest.messages.dataset;

import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.StringUtils;

/**
 * Path parameter identifying cluster data sets.
 */
public class ClusterDataSetIdPathParameter extends MessagePathParameter<IntermediateDataSetID> {

	public static final String KEY = "datasetid";

	public ClusterDataSetIdPathParameter() {
		super(KEY);
	}

	@Override
	protected IntermediateDataSetID convertFromString(String value) {
		return new IntermediateDataSetID(new AbstractID(StringUtils.hexStringToByte(value)));
	}

	@Override
	protected String convertToString(IntermediateDataSetID value) {
		return value.toString();
	}

	@Override
	public String getDescription() {
		return "32-character hexadecimal string value that identifies a cluster data set.";
	}
}
