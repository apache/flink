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

package org.apache.flink.runtime.jobmaster.message;

import java.io.Serializable;

/**
 * The response of the dispose savepoint request to JobManager.
 */
public abstract class DisposeSavepointResponse implements Serializable {

	private static final long serialVersionUID = 6008792963949369567L;

	public static class Success extends DisposeSavepointResponse implements Serializable {

		private static final long serialVersionUID = 1572462960008711415L;
	}

	public static class Failure extends DisposeSavepointResponse implements Serializable {

		private static final long serialVersionUID = -7505308325483022458L;

		private final Throwable cause;

		public Failure(final Throwable cause) {
			this.cause = cause;
		}

		public Throwable getCause() {
			return cause;
		}
	}
}
