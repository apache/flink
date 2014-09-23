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

package org.apache.flink.runtime.protocols;

import java.io.IOException;

import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.protocols.VersionedProtocol;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

/**
 * The input split provider protocol is used to facilitate RPC calls related to the lazy split assignment.
 */
public interface InputSplitProviderProtocol extends VersionedProtocol {

	InputSplit requestNextInputSplit(JobID jobID, JobVertexID vertex) throws IOException;
}
