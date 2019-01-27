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

package org.apache.flink.runtime.rest.messages.taskmanager;

import org.apache.flink.runtime.rest.messages.MessagePathParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;


/**
 * Parameters for range read log REST handler
 *
 * <p>range read log REST handler always requires a {@link LogFileNamePathParameter}.</p>
 * <p>range read log REST handler always requires a {@link FileReadCountQueryParameter}.</p>
 * <p>range read log REST handler always requires a {@link FileReadStartQueryParameter}.</p>
 */
public class FileRangeMessageParameters extends TaskManagerMessageParameters {

	public final LogFileNamePathParameter logFileNamePathParameter = new LogFileNamePathParameter();
	public final TaskManagerIdPathParameter taskManagerIdPathParameter = new TaskManagerIdPathParameter();
	public final FileReadCountQueryParameter fileReadCountQueryParameter = new FileReadCountQueryParameter();
	public final FileReadStartQueryParameter fileReadStartNumQueryParameter = new FileReadStartQueryParameter();

	@Override
	public Collection<MessagePathParameter<?>> getPathParameters() {
		return Collections.unmodifiableCollection(Arrays.asList(
			logFileNamePathParameter,
			taskManagerIdPathParameter
		));
	}

	@Override
	public Collection<MessageQueryParameter<?>> getQueryParameters() {
		return Collections.unmodifiableCollection(Arrays.asList(
			fileReadCountQueryParameter,
			fileReadStartNumQueryParameter
		));
	}
}
