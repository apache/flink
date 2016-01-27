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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import org.apache.flink.python.api.streaming.util.StreamPrinter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.api.PythonPlanBinder;
import static org.apache.flink.python.api.PythonPlanBinder.DEBUG;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON2_BINARY_PATH;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON3_BINARY_PATH;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON_DC_ID;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_PYTHON_PLAN_NAME;
import static org.apache.flink.python.api.PythonPlanBinder.FLINK_TMP_DATA_DIR;
import static org.apache.flink.python.api.PythonPlanBinder.PLANBINDER_CONFIG_BCVAR_COUNT;
import static org.apache.flink.python.api.PythonPlanBinder.PLANBINDER_CONFIG_BCVAR_NAME_PREFIX;
import org.apache.flink.python.api.streaming.util.SerializationUtils.IntSerializer;
import org.apache.flink.python.api.streaming.util.SerializationUtils.StringSerializer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This streamer is used by functions to send/receive data to/from an external python process.
 */
public class PythonStreamer implements Serializable {
	protected static final Logger LOG = LoggerFactory.getLogger(PythonStreamer.class);
	private static final int SIGNAL_BUFFER_REQUEST = 0;
	private static final int SIGNAL_BUFFER_REQUEST_G0 = -3;
	private static final int SIGNAL_BUFFER_REQUEST_G1 = -4;
	private static final int SIGNAL_FINISHED = -1;
	private static final int SIGNAL_ERROR = -2;
	private static final byte SIGNAL_LAST = 32;

	private final int id;
	private final boolean usePython3;
	private final boolean debug;
	private final String planArguments;

	private String inputFilePath;
	private String outputFilePath;

	private Process process;
	private Thread shutdownThread;
	protected ServerSocket server;
	protected Socket socket;
	protected DataInputStream in;
	protected DataOutputStream out;
	protected int port;

	protected PythonSender sender;
	protected PythonReceiver receiver;

	protected StringBuilder msg = new StringBuilder();

	protected final AbstractRichFunction function;

	public PythonStreamer(AbstractRichFunction function, int id, boolean usesByteArray) {
		this.id = id;
		this.usePython3 = PythonPlanBinder.usePython3;
		this.debug = DEBUG;
		planArguments = PythonPlanBinder.arguments.toString();
		sender = new PythonSender();
		receiver = new PythonReceiver(usesByteArray);
		this.function = function;
	}

	/**
	 * Starts the python script.
	 *
	 * @throws IOException
	 */
	public void open() throws IOException {
		server = new ServerSocket(0);
		startPython();
	}

	private void startPython() throws IOException {
		this.outputFilePath = FLINK_TMP_DATA_DIR + "/" + id + this.function.getRuntimeContext().getIndexOfThisSubtask() + "output";
		this.inputFilePath = FLINK_TMP_DATA_DIR + "/" + id + this.function.getRuntimeContext().getIndexOfThisSubtask() + "input";

		sender.open(inputFilePath);
		receiver.open(outputFilePath);

		String path = function.getRuntimeContext().getDistributedCache().getFile(FLINK_PYTHON_DC_ID).getAbsolutePath();
		String planPath = path + FLINK_PYTHON_PLAN_NAME;

		String pythonBinaryPath = usePython3 ? FLINK_PYTHON3_BINARY_PATH : FLINK_PYTHON2_BINARY_PATH;

		try {
			Runtime.getRuntime().exec(pythonBinaryPath);
		} catch (IOException ex) {
			throw new RuntimeException(pythonBinaryPath + " does not point to a valid python binary.");
		}

		if (debug) {
			socket.setSoTimeout(0);
			LOG.info("Waiting for Python Process : " + function.getRuntimeContext().getTaskName()
					+ " Run python " + planPath + planArguments);
		} else {
			process = Runtime.getRuntime().exec(pythonBinaryPath + " -O -B " + planPath + planArguments);
			new StreamPrinter(process.getInputStream()).start();
			new StreamPrinter(process.getErrorStream(), true, msg).start();
		}

		shutdownThread = new Thread() {
			@Override
			public void run() {
				try {
					destroyProcess();
				} catch (IOException ex) {
				}
			}
		};

		Runtime.getRuntime().addShutdownHook(shutdownThread);

		OutputStream processOutput = process.getOutputStream();
		processOutput.write("operator\n".getBytes());
		processOutput.write(("" + server.getLocalPort() + "\n").getBytes());
		processOutput.write((id + "\n").getBytes());
		processOutput.write((inputFilePath + "\n").getBytes());
		processOutput.write((outputFilePath + "\n").getBytes());
		processOutput.flush();

		try { // wait a bit to catch syntax errors
			Thread.sleep(2000);
		} catch (InterruptedException ex) {
		}
		if (!debug) {
			try {
				process.exitValue();
				throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " terminated prematurely." + msg);
			} catch (IllegalThreadStateException ise) { //process still active -> start receiving data
			}
		}

		socket = server.accept();
		in = new DataInputStream(socket.getInputStream());
		out = new DataOutputStream(socket.getOutputStream());
	}

	/**
	 * Closes this streamer.
	 *
	 * @throws IOException
	 */
	public void close() throws IOException {
		try {
			socket.close();
			sender.close();
			receiver.close();
		} catch (Exception e) {
			LOG.error("Exception occurred while closing Streamer. :" + e.getMessage());
		}
		if (!debug) {
			destroyProcess();
		}
		if (shutdownThread != null) {
			Runtime.getRuntime().removeShutdownHook(shutdownThread);
		}
	}

	private void destroyProcess() throws IOException {
		try {
			process.exitValue();
		} catch (IllegalThreadStateException ise) { //process still active
			if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
				int pid;
				try {
					Field f = process.getClass().getDeclaredField("pid");
					f.setAccessible(true);
					pid = f.getInt(process);
				} catch (Throwable e) {
					process.destroy();
					return;
				}
				String[] args = new String[]{"kill", "-9", "" + pid};
				Runtime.getRuntime().exec(args);
			} else {
				process.destroy();
			}
		}
	}

	private void sendWriteNotification(int size, boolean hasNext) throws IOException {
		out.writeInt(size);
		out.writeByte(hasNext ? 0 : SIGNAL_LAST);
		out.flush();
	}

	private void sendReadConfirmation() throws IOException {
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
				Iterator bcv = function.getRuntimeContext().getBroadcastVariable(name).iterator();

				out.write(stringSerializer.serializeWithoutTypeInfo(name));

				while (bcv.hasNext()) {
					out.writeByte(1);
					out.write((byte[]) bcv.next());
				}
				out.writeByte(0);
			}
		} catch (SocketTimeoutException ste) {
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		}
	}

	/**
	 * Sends all values contained in the iterator to the external process and collects all results.
	 *
	 * @param i iterator
	 * @param c collector
	 * @throws IOException
	 */
	public final void streamBufferWithoutGroups(Iterator i, Collector c) throws IOException {
		try {
			int size;
			if (i.hasNext()) {
				while (true) {
					int sig = in.readInt();
					switch (sig) {
						case SIGNAL_BUFFER_REQUEST:
							if (i.hasNext() || sender.hasRemaining(0)) {
								size = sender.sendBuffer(i, 0);
								sendWriteNotification(size, sender.hasRemaining(0) || i.hasNext());
							} else {
								throw new RuntimeException("External process requested data even though none is available.");
							}
							break;
						case SIGNAL_FINISHED:
							return;
						case SIGNAL_ERROR:
							try { //wait before terminating to ensure that the complete error message is printed
								Thread.sleep(2000);
							} catch (InterruptedException ex) {
							}
							throw new RuntimeException(
									"External process for task " + function.getRuntimeContext().getTaskName() + " terminated prematurely due to an error." + msg);
						default:
							receiver.collectBuffer(c, sig);
							sendReadConfirmation();
							break;
					}
				}
			}
		} catch (SocketTimeoutException ste) {
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		}
	}

	/**
	 * Sends all values contained in both iterators to the external process and collects all results.
	 *
	 * @param i1 iterator
	 * @param i2 iterator
	 * @param c collector
	 * @throws IOException
	 */
	public final void streamBufferWithGroups(Iterator i1, Iterator i2, Collector c) throws IOException {
		try {
			int size;
			if (i1.hasNext() || i2.hasNext()) {
				while (true) {
					int sig = in.readInt();
					switch (sig) {
						case SIGNAL_BUFFER_REQUEST_G0:
							if (i1.hasNext() || sender.hasRemaining(0)) {
								size = sender.sendBuffer(i1, 0);
								sendWriteNotification(size, sender.hasRemaining(0) || i1.hasNext());
							}
							break;
						case SIGNAL_BUFFER_REQUEST_G1:
							if (i2.hasNext() || sender.hasRemaining(1)) {
								size = sender.sendBuffer(i2, 1);
								sendWriteNotification(size, sender.hasRemaining(1) || i2.hasNext());
							}
							break;
						case SIGNAL_FINISHED:
							return;
						case SIGNAL_ERROR:
							try { //wait before terminating to ensure that the complete error message is printed
								Thread.sleep(2000);
							} catch (InterruptedException ex) {
							}
							throw new RuntimeException(
									"External process for task " + function.getRuntimeContext().getTaskName() + " terminated prematurely due to an error." + msg);
						default:
							receiver.collectBuffer(c, sig);
							sendReadConfirmation();
							break;
					}
				}
			}
		} catch (SocketTimeoutException ste) {
			throw new RuntimeException("External process for task " + function.getRuntimeContext().getTaskName() + " stopped responding." + msg);
		}
	}
}
