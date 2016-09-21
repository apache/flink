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

package org.apache.flink.runtime.rpc.akka.messages;

import org.apache.flink.runtime.rpc.RpcGateway;

import java.io.Serializable;

/**
 * Message to verify that the {@link org.apache.flink.runtime.rpc.RpcEndpoint} supports the
 * specified {@link RpcGateway}.
 */
public class VerifyRpcGateway implements Serializable {
	private static final long serialVersionUID = 2253701464989577738L;

	private final Class<? extends RpcGateway> clazz;

	public VerifyRpcGateway(Class<? extends RpcGateway> clazz) {
		this.clazz = clazz;
	}

	public Class<? extends RpcGateway> getClazz() {
		return clazz;
	}
}
