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

package org.apache.flink.table.temptable.rpc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.service.LifeCycleAware;
import org.apache.flink.service.ServiceInstance;
import org.apache.flink.service.ServiceRegistry;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.temptable.TableServiceException;
import org.apache.flink.table.temptable.TableServiceOptions;
import org.apache.flink.table.temptable.util.BytesUtil;
import org.apache.flink.table.typeutils.BaseRowSerializer;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.LengthFieldBasedFrameDecoder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Built-in implementation of Table Service Client.
 * Data will be serialized in form of the {@link BinaryRow}.
 */
public class TableServiceClient implements LifeCycleAware {

	private Logger logger = LoggerFactory.getLogger(TableServiceClient.class);

	private List<ServiceInstance> serviceInfoList;

	private String lastTableName = null;

	private int lastPartitionIndex = -1;

	private int lastTablePartitionOffset = 0;

	private Bootstrap bootstrap;

	private ChannelFuture channelFuture;

	private EventLoopGroup eventLoopGroup;

	private TableServiceClientHandler clientHandler;

	private volatile TableServiceBuffer writeBuffer;

	private volatile TableServiceBuffer readBuffer;

	private int readBufferSize;

	private int writeBufferSize;

	private ServiceRegistry registry;

	private String tableServiceId;

	public void setRegistry(ServiceRegistry registry) {
		this.registry = registry;
	}

	public final ServiceRegistry getRegistry() {
		return registry;
	}

	public List<Integer> getPartitions(String tableName) {
		List<ServiceInstance> serviceInfoList = getRegistry().getAllInstances(tableServiceId);
		List<Integer> partitions = new ArrayList<>();
		if (serviceInfoList != null) {
			for (ServiceInstance serviceInfo : serviceInfoList) {
				Bootstrap bootstrap = new Bootstrap();
				TableServiceClientHandler clientHandler = new TableServiceClientHandler();
				ChannelFuture channelFuture = null;
				try {
					bootstrap.group(eventLoopGroup)
						.channel(NioSocketChannel.class)
						.option(ChannelOption.TCP_NODELAY, true)
						.handler(new ChannelInitializer<SocketChannel>() {
							@Override
							protected void initChannel(SocketChannel socketChannel) throws Exception {
								socketChannel.pipeline().addLast(new LengthFieldBasedFrameDecoder(
									65536,
									0,
									Integer.BYTES,
									-Integer.BYTES,
									Integer.BYTES)
								);
								socketChannel.pipeline().addLast(clientHandler);
							}
						});
					channelFuture = bootstrap.connect(serviceInfo.getServiceIp(), serviceInfo.getServicePort()).sync();
					List<Integer> subPartitions = clientHandler.getPartitions(tableName);
					if (subPartitions != null && !subPartitions.isEmpty()) {
						partitions.addAll(subPartitions);
					}
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					throw new TableServiceException(new RuntimeException(e.getMessage(), e));
				} finally {
					if (channelFuture != null) {
						try {
							channelFuture.channel().close().sync();
						} catch (InterruptedException e) {
							logger.error(e.getMessage(), e);
						}
					}
				}
			}
		}
		return partitions;
	}

	public void write(String tableName, int partitionIndex, BaseRow row, BaseRowSerializer baseRowSerializer) throws Exception {
		ensureConnected(tableName, partitionIndex);
		if (writeBuffer == null) {
			writeBuffer = new TableServiceBuffer(tableName, partitionIndex, writeBufferSize);
		}
		byte[] serialized = BytesUtil.serialize(row, baseRowSerializer);
		if (writeBuffer.getByteBuffer().remaining() >= serialized.length) {
			writeBuffer.getByteBuffer().put(serialized);
		} else {
			writeBuffer.getByteBuffer().flip();
			int remaining = writeBuffer.getByteBuffer().remaining();
			byte[] writeBytes = new byte[remaining];
			writeBuffer.getByteBuffer().get(writeBytes);
			writeBuffer.getByteBuffer().clear();
			clientHandler.write(tableName, partitionIndex, writeBytes);
			clientHandler.write(tableName, partitionIndex, serialized);
		}
	}

	public BaseRow readNext(String tableName, int partitionIndex, BaseRowSerializer baseRowSerializer) throws Exception {
		ensureConnected(tableName, partitionIndex);
		if (readBuffer == null) {
			readBuffer = new TableServiceBuffer(tableName, partitionIndex, readBufferSize);
		}

		byte[] buffer = readFromBuffer(Integer.BYTES);

		if (buffer == null) {
			return null;
		}

		int sizeInBytes = BytesUtil.bytesToInt(buffer);

		buffer = readFromBuffer(sizeInBytes);

		if (buffer == null) {
			return null;
		}

		return BytesUtil.deSerialize(buffer, sizeInBytes, baseRowSerializer);
	}

	public void initializePartition(String tableName, int partitionIndex) throws Exception {
		ensureConnected(tableName, partitionIndex);
		clientHandler.initializePartition(tableName, partitionIndex);
	}

	@Override
	public void open(Configuration config) throws Exception{

		tableServiceId = config.getString(TableServiceOptions.TABLE_SERVICE_ID);

		logger.info("TableServiceClient open with tableServiceId = " + tableServiceId);

		if (getRegistry() != null) {
			getRegistry().open(config);
		}

		readBufferSize = config.getInteger(TableServiceOptions.TABLE_SERVICE_CLIENT_READ_BUFFER_SIZE);
		writeBufferSize = config.getInteger(TableServiceOptions.TABLE_SERVICE_CLIENT_WRITE_BUFFER_SIZE);

		eventLoopGroup = new NioEventLoopGroup();
	}

	@Override
	public void close() throws Exception {

		if (writeBuffer != null) {
			writeBuffer.getByteBuffer().flip();
			if (writeBuffer.getByteBuffer().hasRemaining()) {
				ensureConnected(writeBuffer.getTableName(), writeBuffer.getPartitionIndex());
				byte[] writeBytes = new byte[writeBuffer.getByteBuffer().remaining()];
				writeBuffer.getByteBuffer().get(writeBytes);
				clientHandler.write(writeBuffer.getTableName(), writeBuffer.getPartitionIndex(), writeBytes);
				writeBuffer.getByteBuffer().clear();
			}
		}

		if (channelFuture != null) {
			try {
				channelFuture.channel().close().sync();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}
		if (eventLoopGroup != null && !eventLoopGroup.isShutdown()) {
			eventLoopGroup.shutdownGracefully();
		}
	}

	private void ensureConnected(String tableName, int partitionIndex) throws Exception {
		if (tableName.equals(lastTableName) && partitionIndex == lastPartitionIndex) {
			return;
		}

		if (bootstrap != null) {
			try {
				channelFuture.channel().close().sync();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			} finally {
				bootstrap = null;
				channelFuture = null;
			}
		}

		lastTableName = tableName;
		lastPartitionIndex = partitionIndex;
		lastTablePartitionOffset = 0;
		serviceInfoList = getRegistry().getAllInstances(tableServiceId);

		if (serviceInfoList == null || serviceInfoList.isEmpty()) {
			logger.error("fetch serviceInfoList fail");
			throw new TableServiceException(new RuntimeException("serviceInfoList is empty"));
		}

		Collections.sort(serviceInfoList, Comparator.comparing(ServiceInstance::getInstanceId));

		int hashCode = Objects.hash(tableName, partitionIndex);

		int pickIndex = (hashCode % serviceInfoList.size());
		if (pickIndex < 0) {
			pickIndex += serviceInfoList.size();
		}

		ServiceInstance pickedServiceInfo = serviceInfoList.get(pickIndex);

		logger.info("build client with ip = " + pickedServiceInfo.getServiceIp() + ", port = " + pickedServiceInfo.getServicePort());

		bootstrap = new Bootstrap();

		clientHandler = new TableServiceClientHandler();
		bootstrap.group(eventLoopGroup)
			.channel(NioSocketChannel.class)
			.option(ChannelOption.TCP_NODELAY, true)
			.handler(new ChannelInitializer<SocketChannel>() {
				@Override
				protected void initChannel(SocketChannel socketChannel) throws Exception {
					socketChannel.pipeline().addLast(new LengthFieldBasedFrameDecoder(
						65536,
						0,
						Integer.BYTES,
						-Integer.BYTES,
						Integer.BYTES)
					);
					socketChannel.pipeline().addLast(clientHandler);
				}
			});

		try {
			channelFuture = bootstrap.connect(pickedServiceInfo.getServiceIp(), pickedServiceInfo.getServicePort()).sync();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
			throw new TableServiceException(e);
		}

		logger.info("build client end");
	}

	public boolean isReady() {

		List<ServiceInstance> serviceInfoList = getRegistry().getAllInstances(tableServiceId);

		if (serviceInfoList == null || serviceInfoList.isEmpty()) {
			logger.info("serviceInfoList is empty");
			return false;
		}

		String headInstanceId = serviceInfoList.get(0).getInstanceId();

		int totalInstanceNumber = Integer.valueOf(headInstanceId.split("_")[1]);

		if (serviceInfoList.size() != totalInstanceNumber) {
			logger.info("expected " + totalInstanceNumber + " instance, but only " + serviceInfoList.size() + " found");
			return false;
		}

		boolean misMatch = serviceInfoList.stream()
			.anyMatch(serviceInfo -> !serviceInfo.getInstanceId().split("_")[1].equals(totalInstanceNumber + ""));

		if (misMatch) {
			logger.info("mismatch total number of instance");
			return false;
		}

		boolean countMatch = serviceInfoList.stream()
			.map(serviceInfo -> Integer.valueOf(serviceInfo.getInstanceId().split("_")[0]))
			.collect(Collectors.toSet())
			.size() == totalInstanceNumber;

		if (!countMatch) {
			logger.info("some instance is not ready");
			return false;
		}

		return true;
	}

	private byte[] readFromBuffer(int readCount) throws Exception {

		readBuffer.getByteBuffer().flip();
		byte[] bytes;
		int remaining = readBuffer.getByteBuffer().remaining();
		if (remaining >= readCount) {
			bytes = new byte[readCount];
			readBuffer.getByteBuffer().get(bytes);
			readBuffer.getByteBuffer().compact();
		} else {
			bytes = new byte[readCount];
			readBuffer.getByteBuffer().get(bytes, 0, remaining);
			int count = remaining;
			if (count < readCount) {
				boolean success = fillReadBufferFromClient();
				if (success) {
					byte[] bufferRead = readFromBuffer(readCount - count);
					if (bufferRead != null) {
						System.arraycopy(bufferRead, 0, bytes, count, readCount - count);
					} else {
						return null;
					}
				} else {
					return null;
				}
			}
		}
		return bytes;
	}

	private boolean fillReadBufferFromClient() throws Exception {
		byte[] buffer;
		buffer = clientHandler.read(lastTableName, lastPartitionIndex, lastTablePartitionOffset, readBufferSize);

		if (buffer != null && buffer.length > 0) {
			lastTablePartitionOffset += buffer.length;
			readBuffer.getByteBuffer().clear();
			readBuffer.getByteBuffer().put(buffer);
			return true;
		}

		return false;
	}

}
