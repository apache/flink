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

package org.apache.flink.runtime.minicluster;

import java.util.concurrent.CompletableFuture;

/**
 * Interface to control {@link JobExecutor}.
 */
public interface JobExecutorService extends JobExecutor {

	/**
	 * Terminate the given JobExecutorService.
	 *
	 * <p>This method can be implemented asynchronously. Therefore it returns a future
	 * which is completed once the termination has been done.
	 *
	 * @return Termination future which can also contain an exception if the termination went wrong
	 */
	CompletableFuture<?> terminate();
}
