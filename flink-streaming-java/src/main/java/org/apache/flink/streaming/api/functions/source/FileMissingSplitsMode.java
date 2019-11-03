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

package org.apache.flink.streaming.api.functions.source;

/**
 * The mode in which the {@link FileMissingSplitsMode} operates.
 * This can be either {@link #SKIP_MISSING_SPLITS} or {@link #FAIL_ON_MISSING_SPLITS}.
 */
public enum FileMissingSplitsMode {

	/** Skip on file missing splits read if file removed after state restore. */
	SKIP_MISSING_SPLITS,

	/** Fail on file missing splits read. */
	FAIL_ON_MISSING_SPLITS
}
