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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.util.FlinkException;

/**
 * This exception is thrown by the {@link LeaderElectionDriver} when {@link LeaderElectionDriver#writeLeaderInformation}
 * failed or some unexpected changes to the leader storage.
 */
public class LeaderElectionException extends FlinkException {

	private static final long serialVersionUID = 1L;

	public LeaderElectionException(String message) {
		super(message);
	}

	public LeaderElectionException(Throwable cause) {
		super(cause);
	}

	public LeaderElectionException(String message, Throwable cause) {
		super(message, cause);
	}
}
