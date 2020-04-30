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

package org.apache.flink.runtime.registration;

/**
 * Classes which want to be notified about the registration result by the {@link RegisteredRpcConnection}
 * have to implement this interface.
 */
public interface RegistrationConnectionListener<T extends RegisteredRpcConnection<?, ?, S>, S extends RegistrationResponse.Success> {

	/**
	 * This method is called by the {@link RegisteredRpcConnection} when the registration is success.
	 *
	 * @param success The concrete response information for successful registration.
	 * @param connection The instance which established the connection
	 */
	void onRegistrationSuccess(T connection, S success);

	/**
	 * This method is called by the {@link RegisteredRpcConnection} when the registration fails.
	 *
	 * @param failure The exception which causes the registration failure.
	 */
	void onRegistrationFailure(Throwable failure);
}
