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

/**
 * This package contains all Netty-based client/server classes used to query
 * KvState instances.
 *
 * <h2>Server and Client</h2>
 *
 * <p>Both server and client expect received binary messages to contain a frame
 * length field. Netty's {@link io.netty.handler.codec.LengthFieldBasedFrameDecoder}
 * is used to fully receive the frame before giving it to the respective client
 * or server handler.
 *
 * <p>Connection establishment and release happens by the client. The server
 * only closes a connection if a fatal failure happens that cannot be resolved
 * otherwise.
 *
 * <p>The is a single server per task manager and a single client can be shared
 * by multiple Threads.
 *
 * <p>See also:
 * <ul>
 * <li>{@link org.apache.flink.runtime.query.netty.KvStateServer}</li>
 * <li>{@link org.apache.flink.runtime.query.netty.KvStateServerHandler}</li>
 * <li>{@link org.apache.flink.runtime.query.netty.KvStateClient}</li>
 * <li>{@link org.apache.flink.runtime.query.netty.KvStateClientHandler}</li>
 * </ul>
 *
 * <h2>Serialization</h2>
 *
 * <p>The exchanged binary messages have the following format:
 *
 * <pre>
 *                     <------ Frame ------------------------->
 *                    +----------------------------------------+
 *                    |        HEADER (8)      | PAYLOAD (VAR) |
 * +------------------+----------------------------------------+
 * | FRAME LENGTH (4) | VERSION (4) | TYPE (4) | CONTENT (VAR) |
 * +------------------+----------------------------------------+
 * </pre>
 *
 * <p>For frame decoding, both server and client use Netty's {@link
 * io.netty.handler.codec.LengthFieldBasedFrameDecoder}. Message serialization
 * is done via static helpers in {@link org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer}.
 * The serialization helpers return {@link io.netty.buffer.ByteBuf} instances,
 * which are ready to be sent to the client or server respectively as they
 * contain the frame length.
 *
 * <p>See also:
 * <ul>
 * <li>{@link org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer}</li>
 * </ul>
 *
 * <h2>Statistics</h2>
 *
 * <p>Both server and client keep track of request statistics via {@link
 * org.apache.flink.runtime.query.netty.KvStateRequestStats}.
 *
 * <p>See also:
 * <ul>
 * <li>{@link org.apache.flink.runtime.query.netty.KvStateRequestStats}</li>
 * </ul>
 */
package org.apache.flink.runtime.query.netty;
