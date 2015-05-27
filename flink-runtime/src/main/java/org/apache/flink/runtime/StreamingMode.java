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

package org.apache.flink.runtime;

/**
 * The streaming mode defines whether the system starts in streaming mode,
 * or in pure batch mode. Note that streaming mode can execute batch programs
 * as well.
 */
public enum StreamingMode {
	
	/** This mode indicates the system can run streaming tasks, of which batch
	 * tasks are a special case. */
	STREAMING,
	
	/** This mode indicates that the system can run only batch tasks */
	BATCH_ONLY;
}
