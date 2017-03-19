/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.python.api.streaming.plan;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.python.api.streaming.util.StreamPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON2_BINARY_PATH;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON3_BINARY_PATH;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON_PLAN_NAME;
import static org.apache.flink.python.api.PythonPlanBinder.usePython3;

/**
 * Generic class to exchange data during the plan phase.
 */
public class PythonPlanStreamer {

	protected static final Logger LOG = LoggerFactory.getLogger(PythonPlanStreamer.class);

	protected PythonPlanSender sender;
	protected PythonPlanReceiver receiver;

	private Process process;
	private ServerSocket server;
	private Socket socket;

	public Object getRecord() throws IOException {
		return getRecord(false);
	}

	public Object getRecord(boolean normalize) throws IOException {
		return receiver.getRecord(normalize);
	}

	public void sendRecord(Object record) throws IOException {
		sender.sendRecord(record);
	}

	public void open(String tmpPath, String args) throws IOException {
		startPython(tmpPath, args);
	}

	private void startPython(String tmpPath, String args) throws IOException {
		String pythonBinaryPath = usePython3 ? FLINK_PYTHON3_BINARY_PATH : FLINK_PYTHON2_BINARY_PATH;

		try {
			Runtime.getRuntime().exec(pythonBinaryPath);
		} catch (IOException ignored) {
			throw new RuntimeException(pythonBinaryPath + " does not point to a valid python binary.");
		}
		process = Runtime.getRuntime().exec(pythonBinaryPath + " -B " + tmpPath + FLINK_PYTHON_PLAN_NAME + args);

		new StreamPrinter(process.getInputStream()).start();
		new StreamPrinter(process.getErrorStream()).start();

		checkPythonProcessHealth();
	}

	public void startPlanMode() throws IOException {
		server = new ServerSocket(0);
		//If after 5 seconds Python doesn't respond, check to see if the Python process has exited
		server.setSoTimeout(5000);

		process.getOutputStream().write("plan\n".getBytes(ConfigConstants.DEFAULT_CHARSET));
		process.getOutputStream().write((server.getLocalPort() + "\n").getBytes(ConfigConstants.DEFAULT_CHARSET));
		process.getOutputStream().flush();

		socket = server.accept();
		sender = new PythonPlanSender(socket.getOutputStream());
		receiver = new PythonPlanReceiver(socket.getInputStream());
	}

	public void close() {
		if (isPythonRunning()) {
			process.destroy();
		}
		try {
			socket.close();
		} catch (IOException e) {
			LOG.error("Failed to close socket.", e);
		}
	}

	public boolean isPythonRunning() {
		try {
			process.exitValue();
		} catch (NullPointerException ignored) { //exception occurred before process was started
		} catch (IllegalThreadStateException ise) { //process still active
			return true;
		}
		return false;
	}

	private void checkPythonProcessHealth() {
		try {
			int value = process.exitValue();
			if (value != 0) {
				throw new RuntimeException("Plan file caused an error. Check log-files for details.");
			} else {
				throw new RuntimeException("Plan file exited prematurely without an error.");
			}
		} catch (IllegalThreadStateException ignored) {//Process still running
		}
	}
}
