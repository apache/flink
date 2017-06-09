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

package org.apache.flink.runtime.query.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.KvStateServerAddress;
import org.apache.flink.runtime.query.netty.message.KvStateRequestResult;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.query.netty.message.KvStateRequestType;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.junit.AfterClass;
import org.junit.Test;

import java.net.InetAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class KvStateServerTest {

	// Thread pool for client bootstrap (shared between tests)
	private static final NioEventLoopGroup NIO_GROUP = new NioEventLoopGroup();

	private final static int TIMEOUT_MILLIS = 10000;

	@AfterClass
	public static void tearDown() throws Exception {
		if (NIO_GROUP != null) {
			NIO_GROUP.shutdownGracefully();
		}
	}

	/**
	 * Tests a simple successful query via a SocketChannel.
	 */
	@Test
	public void testSimpleRequest() throws Exception {
		KvStateServer server = null;
		Bootstrap bootstrap = null;
		try {
			KvStateRegistry registry = new KvStateRegistry();
			KvStateRequestStats stats = new AtomicKvStateRequestStats();

			server = new KvStateServer(InetAddress.getLocalHost(), 0, 1, 1, registry, stats);
			server.start();

			KvStateServerAddress serverAddress = server.getAddress();
			int numKeyGroups = 1;
			AbstractStateBackend abstractBackend = new MemoryStateBackend();
			DummyEnvironment dummyEnv = new DummyEnvironment("test", 1, 0);
			dummyEnv.setKvStateRegistry(registry);
			AbstractKeyedStateBackend<Integer> backend = abstractBackend.createKeyedStateBackend(
					dummyEnv,
					new JobID(),
					"test_op",
					IntSerializer.INSTANCE,
					numKeyGroups,
					new KeyGroupRange(0, 0),
					registry.createTaskRegistry(new JobID(), new JobVertexID()));

			final KvStateServerHandlerTest.TestRegistryListener registryListener =
					new KvStateServerHandlerTest.TestRegistryListener();

			registry.registerListener(registryListener);

			ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<>("any", IntSerializer.INSTANCE);
			desc.setQueryable("vanilla");

			ValueState<Integer> state = backend.getPartitionedState(
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE,
					desc);

			// Update KvState
			int expectedValue = 712828289;

			int key = 99812822;
			backend.setCurrentKey(key);
			state.update(expectedValue);

			// Request
			byte[] serializedKeyAndNamespace = KvStateRequestSerializer.serializeKeyAndNamespace(
					key,
					IntSerializer.INSTANCE,
					VoidNamespace.INSTANCE,
					VoidNamespaceSerializer.INSTANCE);

			// Connect to the server
			final BlockingQueue<ByteBuf> responses = new LinkedBlockingQueue<>();
			bootstrap = createBootstrap(
					new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4),
					new ChannelInboundHandlerAdapter() {
						@Override
						public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
							responses.add((ByteBuf) msg);
						}
					});

			Channel channel = bootstrap
					.connect(serverAddress.getHost(), serverAddress.getPort())
					.sync().channel();

			long requestId = Integer.MAX_VALUE + 182828L;

			assertTrue(registryListener.registrationName.equals("vanilla"));
			ByteBuf request = KvStateRequestSerializer.serializeKvStateRequest(
					channel.alloc(),
					requestId,
					registryListener.kvStateId,
					serializedKeyAndNamespace);

			channel.writeAndFlush(request);

			ByteBuf buf = responses.poll(TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);

			assertEquals(KvStateRequestType.REQUEST_RESULT, KvStateRequestSerializer.deserializeHeader(buf));
			KvStateRequestResult response = KvStateRequestSerializer.deserializeKvStateRequestResult(buf);

			assertEquals(requestId, response.getRequestId());
			int actualValue = KvStateRequestSerializer.deserializeValue(response.getSerializedResult(), IntSerializer.INSTANCE);
			assertEquals(expectedValue, actualValue);
		} finally {
			if (server != null) {
				server.shutDown();
			}

			if (bootstrap != null) {
				EventLoopGroup group = bootstrap.group();
				if (group != null) {
					group.shutdownGracefully();
				}
			}
		}
	}

	/**
	 * Creates a client bootstrap.
	 */
	private Bootstrap createBootstrap(final ChannelHandler... handlers) {
		return new Bootstrap().group(NIO_GROUP).channel(NioSocketChannel.class)
				.handler(new ChannelInitializer<SocketChannel>() {
					@Override
					protected void initChannel(SocketChannel ch) throws Exception {
						ch.pipeline().addLast(handlers);
					}
				});
	}

}
