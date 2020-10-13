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

package org.apache.flink.connectors.test.common.source;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Flink source that can be controlled by Java RMI.
 *
 * <p>This source is used for generating random records to downstream and recording records into a given local file.
 * In order to control records to generate and lifecycle of the Flink job using this source, it integrates with
 * an Java RMI server so that it could be controlled remotely through RPC (basically controlled by testing framework).
 * </p>
 */
public class ControllableSource
		extends AbstractRichFunction
		implements SourceFunction<String>, CheckpointedFunction, SourceControlRpc {

	private static final Logger LOG = LoggerFactory.getLogger(ControllableSource.class);

	/*---------------- Java RMI configurations ---------------*/
	public static final String RMI_PROP_REGISTRY_FILTER = "sun.rmi.registry.registryFilter";
	public static final String RMI_PROP_SERVER_HOSTNAME = "java.rmi.server.hostname";
	public static final String RMI_REGISTRY_NAME = "SourceControl";
	public static final String RMI_HOSTNAME = "127.0.0.1";
	public static final int RMI_PORT = 15213;
	private Registry rmiRegistry;

	private volatile boolean isRunning = true;
	private volatile boolean isStepping = true;

	private final File recordingFile;
	private BufferedWriter bw;

	private AtomicInteger numElementsToEmit;

	private final SyncLock syncLock = new SyncLock();

	private final String endMark;

	public ControllableSource(String recordingFilePath, String endMark) {
		recordingFile = new File(recordingFilePath);
		this.endMark = endMark;
	}

	/*------------------- Checkpoint related---------------------*/
	@Override
	public void snapshotState(FunctionSnapshotContext context) {

	}

	@Override
	public void initializeState(FunctionInitializationContext context) {

	}


	/*-------------------- Rich function related -----------------*/
	@Override
	public void open(Configuration parameters) throws Exception {
		// Setup Java RMI
		java.lang.System.setProperty(RMI_PROP_REGISTRY_FILTER, "java.**;org.apache.flink.**");
		java.lang.System.setProperty(RMI_PROP_SERVER_HOSTNAME, RMI_HOSTNAME);
		UnicastRemoteObject.exportObject(this, RMI_PORT);
		rmiRegistry = LocateRegistry.createRegistry(RMI_PORT);
		rmiRegistry.bind(RMI_REGISTRY_NAME, this);

		// Setup recording file
		bw = new BufferedWriter(new FileWriter(recordingFile));

		// Setup record counter
		numElementsToEmit = new AtomicInteger(0);
	}

	@Override
	public void run(SourceContext<String> ctx) {

		// Main loop
		while (isRunning) {
			synchronized (syncLock) {
				if (numElementsToEmit.get() > 0) {
					emitAndRecordElement(ctx, generateRandomString(10));
					if (isStepping) {
						numElementsToEmit.decrementAndGet();
					}
				}
			}
		}

		// Ready to finish the job
		// Finish leftover
		synchronized (syncLock) {
			while (numElementsToEmit.get() > 0) {
				emitAndRecordElement(ctx, generateRandomString(10));
				numElementsToEmit.decrementAndGet();
			}

			// Emit end mark before exit
			ctx.collect(endMark);
		}
	}

	@Override
	public void cancel() {
		isRunning = false;
	}

	@Override
	public void close() throws Exception {
		if (bw != null) {
			bw.close();
		}
		UnicastRemoteObject.unexportObject(rmiRegistry, true);
	}

	/*------------------------- Java RMI related ----------------------*/
	@Override
	public void pause() {
		LOG.trace("Received command PAUSE");
		synchronized (syncLock) {
			isStepping = true;
		}
	}

	@Override
	public void next() {
		LOG.trace("Received command NEXT");
		// if main thread is running, just ignore the request
		synchronized (syncLock) {
			if (!isStepping) {
				return;
			}
			numElementsToEmit.incrementAndGet();
		}
	}

	@Override
	public void go() {
		LOG.trace("Received command GO");
		synchronized (syncLock) {
			isStepping = false;
			numElementsToEmit.incrementAndGet();
		}
	}

	@Override
	public void finish() {
		LOG.trace("Received command FINISH");
		isRunning = false;
	}

	static class SyncLock implements Serializable {
	}

	private String generateRandomString(int length) {
		String alphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
				"abcdefghijklmnopqrstuvwxyz" +
				"0123456789";
		StringBuilder sb = new StringBuilder();
		Random random = new Random();
		for (int i = 0; i < length; ++i) {
			sb.append(alphaNumericString.charAt(random.nextInt(alphaNumericString.length())));
		}
		return sb.toString();
	}

	private void emitAndRecordElement(SourceContext<String> ctx, String element) {
		LOG.trace("Emitting element: {}", element);
		ctx.collect(element);
		try {
			bw.append(element).append('\n');
		} catch (IOException e) {
			throw new RuntimeException("Failed to appending BufferedWriter", e);
		}
	}
}
