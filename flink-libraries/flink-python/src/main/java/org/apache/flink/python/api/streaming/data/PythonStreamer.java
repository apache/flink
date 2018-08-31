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

package org.apache.flink.python.api.streaming.data;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.api.PythonOptions;
import org.apache.flink.python.api.streaming.util.SerializationUtils.IntSerializer;
import org.apache.flink.python.api.streaming.util.SerializationUtils.StringSerializer;
import org.apache.flink.python.api.streaming.util.StreamPrinter;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON_DC_ID;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON_PLAN_NAME;
import static org.apache.flink.python.api.PythonPlanBinder.PLANBINDER_CONFIG_BCVAR_COUNT;
import static org.apache.flink.python.api.PythonPlanBinder.PLANBINDER_CONFIG_BCVAR_NAME_PREFIX;
import static org.apache.flink.python.api.PythonPlanBinder.PLAN_ARGUMENTS_KEY;

/**
 * This streamer is used by functions to send/receive data to/from an external python process.
 */
public class PythonStreamer<S extends PythonSender, OUT> implements Serializable {
	protected static final Logger LOG = LoggerFactory.getLogger(PythonStreamer.class);
	private static final long serialVersionUID = -2342256613658373170L;

	protected static final int SIGNAL_BUFFER_REQUEST = 0;
	protected static final int SIGNAL_BUFFER_REQUEST_G0 = -3;
	protected static final int SIGNAL_BUFFER_REQUEST_G1 = -4;
	protected static final int SIGNAL_FINISHED = -1;
	protected static final int SIGNAL_ERROR = -2;
	protected static final byte SIGNAL_LAST = 32;

	private final Configuration config;
	private final int envID;
	private final int setID;

	private transient Process process;
	private transient Thread shutdownThread;
	protected transient ServerSocket server;
	protected transient Socket socket;
	protected transient DataInputStream in;
	protected transient DataOutputStream out;
	protected int port;

	protected S sender;
	protected PythonReceiver<OUT> receiver;

	protected AtomicReference<String> msg = new AtomicReference<>();

	protected final AbstractRichFunction function;

	protected transient Thread outPrinter;
	protected transient Thread errorPrinter;

	public PythonStreamer(AbstractRichFunction function, Configuration config, int envID, int setID, boolean usesByteArray, S sender) {
		this.config = config;
		this.envID = envID;
		this.setID = setID;
		this.receiver = new PythonReceiver<>(config, usesByteArray);
		this.function = function;
		this.sender = sender;
	}

	/**
	 * Starts the python script.
	 *
	 * @throws IOException
	 */
	public void open() throws IOException {
		server = new ServerSocket(0);
		server.setSoTimeout(50);
		startPython();
	}

	private void startPython() throws IOException {
		String tmpDir = config.getString(PythonOptions.DATA_TMP_DIR);
		if (tmpDir == null) {
			tmpDir = System.getProperty("java.io.tmpdir");
		}
		File outputFile = new File(tmpDir, envID + "_" + setID + this.function.getRuntimeContext().getIndexOfThisSubtask() + "_output");
		File inputFile = new File(tmpDir, envID + "_" + setID + this.function.getRuntimeContext().getIndexOfThisSubtask() + "_input)");

		sender.open(inputFile);
		receiver.open(outputFile);

		String path = function.getRuntimeContext().getDistributedCache().getFile(FLINK_PYTHON_DC_ID).getAbsolutePath();

		String planPath = path + FLINK_PYTHON_PLAN_NAME;

		String pythonBinaryPath = config.getString(PythonOptions.PYTHON_BINARY_PATH);

		String arguments = config.getString(PLAN_ARGUMENTS_KEY, "");
		process = Runtime.getRuntime().exec(pythonBinaryPath + " -O -B " + planPath + arguments);
		outPrinter = new Thread(new StreamPrinter(process.getInputStream()));
		outPrinter.start();
		errorPrinter = new Thread(new StreamPrinter(process.getErrorStream(), msg));
		errorPrinter.start();

		shutdownThread = ShutdownHookUtil.addShutdownHook(
			() -> destroyProcess(process),
			getClass().getSimpleName(),
			LOG);

		OutputStream processOutput = process.getOutputStream();
		processOutput.write("operator\n".getBytes(ConfigConstants.DEFAULT_CHARSET));
		processOutput.write((envID + "\n").getBytes(ConfigConstants.DEFAULT_CHARSET));
		processOutput.write((setID + "\n").getBytes(ConfigConstants.DEFAULT_CHARSET));
		processOutput.write(("" + server.getLocalPort() + "\n").getBytes(ConfigConstants.DEFAULT_CHARSET));
		processOutput.write((this.function.getRuntimeContext().getIndexOfThisSubtask() + "\n")
			.getBytes(ConfigConstants.DEFAULT_CHARSET));
		processOutput.write(((config.getLong(PythonOptions.MMAP_FILE_SIZE) << 10) + "\n").getBytes(ConfigConstants.DEFAULT_CHARSET));
		processOutput.write((inputFile + "\n").getBytes(ConfigConstants.DEFAULT_CHARSET));
		processOutput.write((outputFile + "\n").getBytes(ConfigConstants.DEFAULT_CHARSET));
		processOutput.flush();

		while (true) {
			try {
				socket = server.accept();
				break;
			} catch (SocketTimeoutException ignored) {
				checkPythonProcessHealth();
			}
		}
		in = new DataInputStream(socket.getInputStream());
		out = new DataOutputStream(socket.getOutputStream());
	}

	private void checkPythonProcessHealth() {
		try {
			int value = process.exitValue();
			try {
				outPrinter.join();
			} catch (InterruptedException ignored) {
				outPrinter.interrupt();
				Thread.interrupted();
			}
			try {
				errorPrinter.join();
			} catch (InterruptedException ignored) {
				errorPrinter.interrupt();
				Thread.interrupted();
			}
			if (value != 0) {
				throw new RuntimeException("Plan file caused an error. Check log-files for details." + msg.get());
			} else {
				throw new RuntimeException("Plan file exited prematurely without an error." + msg.get());
			}
		} catch (IllegalThreadStateException ignored) {//Process still running
		}
	}

	/**
	 * Closes this streamer.
	 *
	 * @throws IOException
	 */
	public void close() throws IOException {
		Throwable throwable = null;

		try {
			socket.close();
			sender.close();
			receiver.close();
		} catch (Throwable t) {
			throwable = t;
		}

		try {
			destroyProcess(process);
		} catch (Throwable t) {
			throwable = ExceptionUtils.firstOrSuppressed(t, throwable);
		}

		ShutdownHookUtil.removeShutdownHook(shutdownThread, getClass().getSimpleName(), LOG);

		ExceptionUtils.tryRethrowIOException(throwable);
	}

	public static void destroyProcess(Process process) throws IOException {
		try {
			process.exitValue();
		} catch (IllegalThreadStateException ignored) { //process still active
			if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
				int pid;
				try {
					Field f = process.getClass().getDeclaredField("pid");
					f.setAccessible(true);
					pid = f.getInt(process);
				} catch (Throwable ignore) {
					process.destroy();
					return;
				}
				String[] args = new String[]{"kill", "-9", String.valueOf(pid)};
				Runtime.getRuntime().exec(args);
			} else {
				process.destroy();
			}
		}
	}

	protected void sendWriteNotification(int size, boolean hasNext) throws IOException {
		out.writeInt(size);
		out.writeByte(hasNext ? 0 : SIGNAL_LAST);
		out.flush();
	}

	protected void sendReadConfirmation() throws IOException {
		out.writeByte(1);
		out.flush();
	}

	/**
	 * Sends all broadcast-variables encoded in the configuration to the external process.
	 *
	 * @param config configuration object containing broadcast-variable count and names
	 * @throws IOException
	 */
	public final void sendBroadCastVariables(Configuration config) throws IOException {
		try {
			int broadcastCount = config.getInteger(PLANBINDER_CONFIG_BCVAR_COUNT, 0);

			String[] names = new String[broadcastCount];

			for (int x = 0; x < names.length; x++) {
				names[x] = config.getString(PLANBINDER_CONFIG_BCVAR_NAME_PREFIX + x, null);
			}

			out.write(new IntSerializer().serializeWithoutTypeInfo(broadcastCount));

			StringSerializer stringSerializer = new StringSerializer();
			for (String name : names) {
				Iterator<byte[]> bcv = function.getRuntimeContext().<byte[]>getBroadcastVariable(name).iterator();

				out.write(stringSerializer.serializeWithoutTypeInfo(name));

				while (bcv.hasNext()) {
					out.writeByte(1);
					out.write(bcv.next());
				}
				out.writeByte(0);
			}
		} catch (SocketTimeoutException ignored) {
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		}
	}
}
