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

package org.apache.flink.table.runtime.functions.python;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.python.api.PythonOptions;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.UserDefinedFunction;
import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.errorcode.TableErrors;
import org.apache.flink.table.runtime.functions.BuildInScalarFunctions;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Python UDF utilities.
 */
public class PythonUDFUtil {
	private static final Logger LOG = LoggerFactory.getLogger(PythonUDFUtil.class);

	public static final int PROTOCAL_VER = 1;
	public static final int SCALAR_UDF = 1;
	public static final int SCALAR_UDF_RESULT = 2;
	public static final int PYTHON_EXCEPTION_THROWN = 3;
	public static final int EXIT_STREAM = 4;
	public static final int EXIT_PYTHON_PROCESS = 5;

	// key for distributed cached python files
	public static final String PYFLINK_CACHED_USR_LIB_IDS = "PYFLINK_SQL_CACHED_USR_LIB_IDS";
	public static final String PYFLINK_LIB_ZIP_FILENAME = "python-source.zip";

	// by contract,  the zip file of python virtualenv should be end with 'venv.zip'.
	// And, the zipped folder name is 'venv'
	public static final String VIRTUALEVN_ZIP_FILENAME = "venv.zip";
	public static final String VIRTUALEVN_FOLDER_NAME = "venv";

	public static final String PYFLINK_SQL_WORKER = "flink.sql.worker";
	public static final String PYFLINK_SQL_REGISTER = "flink.sql.register";

	public static final String PYUDF_PREFIX = "python ";

	private static File pyWorkingDir;
	private static Process pyProcess = null;
	private static volatile int pySockPort = -1;

	private static final int RETRY = 2;

	public static Socket createWorkerSocket(FunctionContext ctx) {

		Socket workerSocket = null;
		for (int p = 0; p < RETRY; p++) {

			setUpPythonServerIfNeeded(ctx);

			for (int s = 0; s < RETRY; s++) {
				try {
					workerSocket = new Socket("127.0.0.1", pySockPort);

					// long connection, and ACK server to see if server is alive
					workerSocket.setKeepAlive(true);
					// workerSocket.setSoTimeout(1000);

					return workerSocket;
				} catch (Exception e) {
					String err = TableErrors.INST.sqlPythonCreateSocketError(e.getMessage());
					LOG.error(err);
				}
			}

			// if can't connect to python which may be crashed
			if (pyProcess != null) {
				pyProcess.destroyForcibly();
				pyProcess = null;
				pySockPort = -1;
			}
			if (p == RETRY) {
				throw new RuntimeException(TableErrors.INST.sqlPythonCreateSocketError(
					"Have tried twice, and can't connect to python. "));
			}
		}

		return workerSocket;
	}

	/**
	 *  Start the python process, shake hands with it so that we know the
	 *  port of the python server socket.  Only create s single Python process for
	 *  a JVM instance.
	 */
	private static synchronized void setUpPythonServerIfNeeded (FunctionContext ctx) {
		if (pyProcess != null && pyProcess.isAlive()) {
			return;
		}

		ServerSocket serverSocket = null;
		try {
			// only once
			if (pyWorkingDir == null || !pyWorkingDir.exists()) {
				Map<String, File> usrFiles = getUserFilesFromDistributedCache(ctx);
				pyWorkingDir = preparePythonWorkingDir(usrFiles);
			}

			// Use this server socket for shaking hands with python process
			serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
			int localPort = serverSocket.getLocalPort();

			pyProcess = startPythonProcess(pyWorkingDir, PYFLINK_SQL_WORKER, localPort);

			// assume that Python process will be ready in 10s
			serverSocket.setSoTimeout(10000);
			Socket shakeHandSock = serverSocket.accept();

			DataInputStream in = new DataInputStream(
				new BufferedInputStream(shakeHandSock.getInputStream()));

			// Big endian. See worker.py
			int port = in.readInt();
			pySockPort = port;

			// after shake hands with Python, python will serve as a socket server.
			// so, close this.
			serverSocket.close();
		} catch (Exception e) {
			String err = TableErrors.INST.sqlPythonProcessError(e.getMessage());
			LOG.error(err);
			throw new RuntimeException(err);
		} finally {
			if (serverSocket != null) {
				try {
					serverSocket.close();
				} catch (IOException e) {
					LOG.warn(e.getMessage());
				}
			}
		}
	}

	/**
	 * Register python UDF, and check the return type at compiling stage.
	 * @param tEnv
	 * @param usrFilePaths
	 * @param pyUDFs
	 * @return the registered python functions
	 * @throws RuntimeException
	 */
	public static Map<String, UserDefinedFunction> registerPyUdfsToTableEnvironment(
		TableEnvironment tEnv,
		ArrayList<String> usrFilePaths,
		Map<String, String> pyUDFs) throws RuntimeException {
		Map<String, UserDefinedFunction> pyUdfs = new HashMap<>();
		ServerSocket serverSocket = null;
		try {
			Map<String, File> usrFiles = new HashMap<>();
			for (String path : usrFilePaths) {
				File f = new File(path);
				usrFiles.put(f.getName(), f);
			}
			File pyWorkDir = preparePythonWorkingDir(usrFiles);

			// Use this server socket for shaking hands with python process
			serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
			int localPort = serverSocket.getLocalPort();

			Process regPyProcess = startPythonProcess(pyWorkDir, PYFLINK_SQL_REGISTER, localPort);
			assert regPyProcess.isAlive();

			// get the return type of udf from python side
			serverSocket.setSoTimeout(1000);
			Socket sock = serverSocket.accept();

			// send function list to python side
			DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(sock.getOutputStream(), 8192));

			ByteArrayOutputStream buff = new ByteArrayOutputStream();
			DataOutputStream outBuff = new DataOutputStream(buff);

			int nums = pyUDFs.size();
			outBuff.writeShort(nums);
			for (Map.Entry<String, String> entry : pyUDFs.entrySet()) {
				outBuff.writeUTF(entry.getValue());
			}
			out.writeLong(buff.size());
			out.write(buff.toByteArray());
			out.flush();

			// get types info of UDFs from python side
			DataInputStream in = new DataInputStream(
				new BufferedInputStream(sock.getInputStream()));
			int dataLength = (int) in.readLong();
			byte[] udfTypesData = new byte[dataLength];
			in.read(udfTypesData);

			serverSocket.close();
			serverSocket = null;
			regPyProcess.destroyForcibly();

			ArrayList<InternalType> internalTypes = parserPythonUdfTypesInfo(udfTypesData);
			assert internalTypes.size() == pyUDFs.size();

			// register all the python udf functions to tEnv
			int idx = 0;
			for (Map.Entry<String, String> entry : pyUDFs.entrySet()) {
				String sqlName = entry.getKey();
				String pyName = entry.getValue();
				PythonScalarFunction scalarFunction =
					new PythonScalarFunction(sqlName, pyName, internalTypes.get(idx));
				tEnv.registerFunction(sqlName, scalarFunction);
				pyUdfs.put(sqlName, scalarFunction);
				idx++;
			}
		}
		catch (Exception ex) {
			throw new RuntimeException("Can't compile the python UDFs: " + ex.getMessage());
		}
		finally {
			if (serverSocket != null) {
				try {
					serverSocket.close();
				} catch (Exception ex) {
					// ignore it
				}
			}
		}
		return pyUdfs;
	}

	/**
	 *  parse the return types for python UDFs at compiling stage.
	 * @param udfTypesData
	 * @return internal types
	 * @throws Exception
	 */
	private static ArrayList<InternalType> parserPythonUdfTypesInfo(byte[] udfTypesData) throws Exception {
		ByteArrayInputStream buff = new ByteArrayInputStream(udfTypesData);
		DataInputStream in = new DataInputStream(buff);

		int nums = in.readShort();

		ArrayList<InternalType> types = new ArrayList<>();
		ArrayList<String> errorMessages = new ArrayList<>();
		for (int i = 0; i < nums; i++) {
			int type = (in.read() & 0xFF);
			if (type > 0x80) { 					 // negative byte
				errorMessages.add(in.readUTF()); // read error message
			}
			else if (type == PythonSerDesTypes.DECIMAL.ordinal()) {
				int precision = (int) in.readShort();
				int scale = (int) in.readShort();
				types.add(DataTypes.createDecimalType(precision, scale));
			}
			else if (type == PythonSerDesTypes.STRING.ordinal()) {
				types.add(DataTypes.STRING);
			}
			else if (type == PythonSerDesTypes.BOOLEAN.ordinal()) {
				types.add(DataTypes.BOOLEAN);
			}
			else if (type == PythonSerDesTypes.SHORT.ordinal()) {
				types.add(DataTypes.SHORT);
			}
			else if (type == PythonSerDesTypes.BYTE.ordinal()) {
				types.add(DataTypes.BYTE);
			}
			else if (type == PythonSerDesTypes.INT.ordinal()) {
				types.add(DataTypes.INT);
			}
			else if (type == PythonSerDesTypes.LONG.ordinal()) {
				types.add(DataTypes.LONG);
			}
			else if (type == PythonSerDesTypes.FLOAT.ordinal()) {
				types.add(DataTypes.FLOAT);
			}
			else if (type == PythonSerDesTypes.DOUBLE.ordinal()) {
				types.add(DataTypes.DOUBLE);
			}
			else if (type == PythonSerDesTypes.BYTES.ordinal()) {
				types.add(DataTypes.BYTE_ARRAY);
			}
			else if (type == PythonSerDesTypes.DATE.ordinal()) {
				types.add(DataTypes.DATE);
			}
			else if (type == PythonSerDesTypes.TIME.ordinal()) {
				types.add(DataTypes.TIME);
			}
			else if (type == PythonSerDesTypes.TIMESTAMP.ordinal()) {
				types.add(DataTypes.TIMESTAMP);
			}
		}

		// some python udf has errors at compiling stage. combine the error info
		if (errorMessages.size() > 0) {
			StringBuilder sb = new StringBuilder();
			for (String errMsg: errorMessages) {
				sb.append(errMsg);
			}
			throw new RuntimeException(sb.toString());
		}

		return types;
	}

	private static HashMap<String, File> getUserFilesFromDistributedCache(FunctionContext ctx)
		throws FileNotFoundException {
		// key(fileName), distributed file
		HashMap<String, File> usrFiles = new HashMap<>();
		String usrLibIds = ctx.getJobParameter(PYFLINK_CACHED_USR_LIB_IDS, "");

		// copy all user's files from distributed cache to tmp folder
		for (String fileName : usrLibIds.split(",")) {
			File dcFile = ctx.getCachedFile(fileName);
			if (dcFile == null || !dcFile.exists()) {
				throw new FileNotFoundException("User's python lib file " + fileName + " does not exist.");
			}
			usrFiles.put(fileName, dcFile);
		}

		return usrFiles;
	}

	/**
	 * Prepare the working directory for python, setup the libraries & user's udf files.
	 * @param usrFiles
	 * @return working directory
	 * @throws IOException
	 */
	private static File preparePythonWorkingDir(Map<String, File> usrFiles) throws IOException {

		File pyWorkDir;

		String tmpFilesDir = System.getProperty("java.io.tmpdir") +
			File.separator + "pyflink_tmp_" + UUID.randomUUID();

		// 1. setup temporary local directory for flink python library and user files
		Path tmpFileDirPath = new Path(tmpFilesDir);
		FileSystem fs = tmpFileDirPath.getFileSystem();
		if (fs.exists(tmpFileDirPath)) {
			fs.delete(tmpFileDirPath, true);
		}
		tmpFileDirPath.getFileSystem().mkdirs(tmpFileDirPath);

		// copy all user's files from distributed cache to tmp folder
		for (Map.Entry<String, File> entry : usrFiles.entrySet()) {
			if (entry.getKey().endsWith(VIRTUALEVN_ZIP_FILENAME)) {
				prepareVirtualEnvFiles(entry.getValue().getAbsolutePath(), tmpFileDirPath.toUri().toString());
			}
			else {
				// use the original name (key)
				Path targetFilePath = new Path(tmpFileDirPath, entry.getKey());
				FileCache.copy(new Path(entry.getValue().toURI().toString()), targetFilePath, false);
			}
		}

		// 2. extract flink's lib from resource
		final String pythonSource = PythonUDFUtil.PYFLINK_LIB_ZIP_FILENAME;
		ClassLoader classLoader = PythonOptions.class.getClassLoader();
		InputStream in = classLoader.getResourceAsStream(pythonSource);
		if (in == null) {
			String err = "Can't extract python library files from resource..";
			LOG.error(err);
			throw new IOException(err);
		}
		File targetFile = new File(tmpFileDirPath.getPath() + File.separator + pythonSource);
		java.nio.file.Files.copy(
			in,
			targetFile.toPath(),
			StandardCopyOption.REPLACE_EXISTING);

		IOUtils.closeQuietly(in);

		pyWorkDir = new File(tmpFileDirPath.toUri().toString());

		return pyWorkDir;
	}

	private static void prepareVirtualEnvFiles(String venvZipFilePath, String pythonDir) {
		try {
			// ZipInputStream won't keep the permission of the files
			// apache compress does. But, java OutputStream can't open hidden files.
			// here, use shell commands to unzip it.
			String[] unzipCmd = {
				"unzip",
				"-qq",
				"-o",
				venvZipFilePath,
				"-d",
				pythonDir
			};
			ProcessBuilder pb = new ProcessBuilder();
			pb.command(unzipCmd);
			Process p = pb.start();

			redirectStreamsToStderr(p.getInputStream(), p.getErrorStream());
			//Runtime.getRuntime().addShutdownHook(new ShutDownPythonHook(p, null));

			p.waitFor(1, TimeUnit.MINUTES);
			if (!p.isAlive()) {
				p.destroyForcibly();
			}

			String pythonExec = pythonDir + "/venv/bin/python";
			File pyExec = new File(pythonExec);
			if (!pyExec.exists()) {
				throw new RuntimeException("Can not setup virtualenv");
			}
		}
		catch (Exception ex) {
			throw new RuntimeException("Can't prepare virtualenv for python, please check your venv.zip. " + ex.getMessage());
		}
	}

	/**
	 *  Setup python environment and start the python process.
	 */
	private static synchronized Process startPythonProcess(File pyWorkDir, String pyWorker, int javaPort) throws Exception {
		boolean virtualenvEnabled = false;

		// building PYTHONPATH string
		StringBuilder pythonPathEnv = new StringBuilder();
		if (System.getenv("PYTHONPATH") != null) {
			pythonPathEnv.append(System.getenv("PYTHONPATH"));
			pythonPathEnv.append(File.pathSeparator);
		}

		String pythonDir = pyWorkDir.getAbsolutePath();
		File pyflinkZip = new File(pythonDir, PYFLINK_LIB_ZIP_FILENAME);
		if (!pyflinkZip.exists()) {
			String err = PYFLINK_LIB_ZIP_FILENAME + "doesn't exist!";
			LOG.error(err);
			throw new Exception(err);
		}
		pythonPathEnv.append(pyflinkZip.getAbsolutePath());

		String[] fileList = pyWorkDir.list();
		for (String f: fileList) {
			if (f.endsWith(".zip")
				&& !f.endsWith(PYFLINK_LIB_ZIP_FILENAME)
				&& !f.endsWith(VIRTUALEVN_ZIP_FILENAME)) {
				File file = new File(pythonDir, f);
				pythonPathEnv.append(File.pathSeparator);
				pythonPathEnv.append(file.getAbsolutePath());
			}
			else if (VIRTUALEVN_FOLDER_NAME.equals(f)) {
				virtualenvEnabled = true;
			}
		}
		pythonPathEnv.append(File.pathSeparator);
		pythonPathEnv.append(pythonDir);

		// default executable command
		String pythonExec = "python";
		String[] commands;
		if (virtualenvEnabled) {
			pythonExec = pythonDir + "/venv/bin/python";
		}

		commands = new String[] {
			pythonExec,
			"-m",
			pyWorker,
			" " + javaPort
		};

		ProcessBuilder pb = new ProcessBuilder();
		Map<String, String> env = pb.environment();
		StringBuilder pathVar = new StringBuilder();
		pathVar.append(pythonDir + "/venv/bin/");
		pathVar.append(File.pathSeparator);
		pathVar.append(env.get("PATH"));
		env.put("PATH", pathVar.toString());
		env.put("PYTHONPATH", pythonPathEnv.toString());
		pb.command(commands);
		Process p = pb.start();

		// Redirect python worker stdout and stderr
		redirectStreamsToStderr(p.getInputStream(), p.getErrorStream());

		// 1 s
		p.waitFor(1000, TimeUnit.MILLISECONDS);
		if (!p.isAlive()) {
			throw new RuntimeException("Can't start python process!");
		}

		// Make sure that the python sub process will be killed when JVM exit
		Runtime.getRuntime().addShutdownHook(new ShutDownPythonHook(p, pythonDir));

		return p;
	}

	private static void redirectStreamsToStderr(InputStream stdout, InputStream stderr) {
		try {
			new RedirectThread(stdout, System.err).start();
			new RedirectThread(stderr, System.err).start();
		}
		catch (Exception ex) {
			LOG.warn(ex.getMessage());
		}
	}

	public static void sendCallRequest(String pyFunctionName, DataOutputStream out, Object... args) throws IOException {

		ByteArrayOutputStream cmdBuff = new ByteArrayOutputStream();
		ByteArrayOutputStream argsTypesBuff = new ByteArrayOutputStream();
		ByteArrayOutputStream argsDataBuff = new ByteArrayOutputStream();

		DataOutputStream cmdOut = new DataOutputStream(cmdBuff);
		DataOutputStream argsTypesOut = new DataOutputStream(argsTypesBuff);
		DataOutputStream argsDataOut = new DataOutputStream(argsDataBuff);

		// Header & commands & args num
		cmdOut.writeInt(PythonUDFUtil.PROTOCAL_VER);  // protocol version
		cmdOut.writeInt(PythonUDFUtil.SCALAR_UDF);    // action
		cmdOut.writeUTF(pyFunctionName);              // with length
		cmdOut.writeShort(args.length);               // args num

		// types & data of arguments
		for (Object a : args) {
			if (a == null) {
				argsTypesOut.writeByte(PythonSerDesTypes.NONE.ordinal());
			}
			if (a instanceof String) {
				argsTypesOut.writeByte(PythonSerDesTypes.STRING.ordinal());
				argsDataOut.writeUTF((String) a);
			}
			else if (a instanceof Boolean) {
				argsTypesOut.writeByte(PythonSerDesTypes.BOOLEAN.ordinal());
				argsDataOut.writeBoolean((Boolean) a);
			}
			else if (a instanceof Short) {
				argsTypesOut.writeByte(PythonSerDesTypes.SHORT.ordinal());
				argsDataOut.writeShort((Short) a);
			}
			else if (a instanceof Byte) {
				argsTypesOut.writeByte(PythonSerDesTypes.BYTE.ordinal());
				argsDataOut.writeByte((Byte) a);
			}
			else if (a instanceof Integer) {
				argsTypesOut.writeByte(PythonSerDesTypes.INT.ordinal());
				argsDataOut.writeInt((Integer) a);
			}
			else if (a instanceof Long) {
				argsTypesOut.writeByte(PythonSerDesTypes.LONG.ordinal());
				argsDataOut.writeLong((Long) a);
			}
			else if (a instanceof Float) {
				argsTypesOut.writeByte(PythonSerDesTypes.FLOAT.ordinal());
				argsDataOut.writeFloat((Float) a);
			}
			else if (a instanceof Double) {
				argsTypesOut.writeByte(PythonSerDesTypes.DOUBLE.ordinal());
				argsDataOut.writeDouble((Double) a);
			}
			else if (a instanceof byte[]) {
				argsTypesOut.writeByte(PythonSerDesTypes.BYTES.ordinal());

				byte[] bytes = (byte[]) a;
				int len = bytes.length;
				argsDataOut.writeShort(len);
				argsDataOut.write(bytes);
			}
			else if (a instanceof java.sql.Date) {
				argsTypesOut.writeByte(PythonSerDesTypes.DATE.ordinal());

				java.sql.Date date = (java.sql.Date) a;
				argsDataOut.writeInt(BuildInScalarFunctions.toInt(date));
			}
			else if (a instanceof java.sql.Time) {
				argsTypesOut.writeByte(PythonSerDesTypes.TIME.ordinal());

				java.sql.Time time = (java.sql.Time) a;
				argsDataOut.writeInt(BuildInScalarFunctions.toInt(time));
			}
			else if (a instanceof java.sql.Timestamp) {
				argsTypesOut.writeByte(PythonSerDesTypes.TIMESTAMP.ordinal());

				java.sql.Timestamp ts = (java.sql.Timestamp) a;
				argsDataOut.writeLong(ts.getTime());
			}
			else if (a instanceof java.math.BigDecimal) {
				argsTypesOut.writeByte(PythonSerDesTypes.DECIMAL.ordinal());
				argsDataOut.writeUTF(a.toString());
			}
		}

		long dataLength = cmdBuff.size() + argsTypesBuff.size() + argsDataBuff.size();

		// write length (long type) into the stream,
		// so that python side know how to recv all the data.
		out.writeLong(dataLength);

		// write all the raw data into the stream
		out.write(cmdBuff.toByteArray());
		out.write(argsTypesBuff.toByteArray());
		out.write(argsDataBuff.toByteArray());
		out.flush();
	}

	public static Object getResult(DataInputStream in) throws IOException {
		int resType = (int) in.readByte();
		Object res = null;
		if (resType == PythonSerDesTypes.NONE.ordinal()) {
			res = null;
		}
		else if (resType == PythonSerDesTypes.STRING.ordinal()) {
			// just return UTF-8 string, to avoid conversion
			int len = in.readShort();
			byte[] utf8bytes = new byte[len];
			in.read(utf8bytes);
			res = BinaryString.fromBytes(utf8bytes);
		}
		else if (resType == PythonSerDesTypes.BOOLEAN.ordinal()) {
			res = in.readBoolean();
		}
		else if (resType == PythonSerDesTypes.SHORT.ordinal()) {
			res = in.readShort();
		}
		else if (resType == PythonSerDesTypes.BYTE.ordinal()) {
			res = in.readByte();
		}
		else if (resType == PythonSerDesTypes.INT.ordinal()) {
			res = in.readInt();
		}
		else if (resType == PythonSerDesTypes.LONG.ordinal()) {
			res = in.readLong();
		}
		else if (resType == PythonSerDesTypes.FLOAT.ordinal()) {
			res = in.readFloat();
		}
		else if (resType == PythonSerDesTypes.DOUBLE.ordinal()) {
			res = in.readDouble();
		}
		else if (resType == PythonSerDesTypes.BYTES.ordinal()) {
			int len = in.readUnsignedShort();
			byte[] bytes = new byte[len];
			in.read(bytes);
			res = bytes;
		}
		else if (resType == PythonSerDesTypes.DATE.ordinal()) {
			res = in.readInt();
		}
		else if (resType == PythonSerDesTypes.TIME.ordinal()) {
			res = in.readInt();
		}
		else if (resType == PythonSerDesTypes.TIMESTAMP.ordinal()) {
			res = in.readLong();
		}
		else if (resType == PythonSerDesTypes.DECIMAL.ordinal()) {
			String s = in.readUTF();
			res = new java.math.BigDecimal(s);
		}

		return res;
	}

	/**
	 *  types for python udf ser/des.
	 */
	enum PythonSerDesTypes {
		/**
		 * indicate java null, python None type.
		 */
		NONE,

		/**
		 * indicate utf-8 string.
		 */
		STRING,

		/**
		 * indicate boolean type.
		 */
		BOOLEAN,

		/**
		 * indicate short (2 bytes) integer.
		 */
		SHORT,

		/**
		 * indicate tiny (1 byte) integer.
		 */
		BYTE,

		/**
		 * indicate 4 bytes integer.
		 */
		INT,

		/**
		 * indicate 8 bytes integer.
		 */
		LONG,

		/**
		 * indicate float.
		 */
		FLOAT,

		/**
		 * indicate double.
		 */
		DOUBLE,

		/**
		 * indicate binary.
		 */
		BYTES,

		/**
		 * indicate date type. internally, it is epoch days.
		 */
		DATE,

		/**
		 * indicate time type. internally, it is epoch milliseconds.
		 */
		TIME,

		/**
		 * indicate timestamp type. internally, it is epoch milliseconds.
		 */
		TIMESTAMP,

		/**
		 * indicate decimal.
		 */
		DECIMAL
	}

	static class RedirectThread extends Thread {
		InputStream in;
		OutputStream out;
		public RedirectThread(InputStream in, OutputStream out) {
			setDaemon(true);
			this.in = in;
			this.out = out;
		}

		@Override
		public void run() {
			try {
				byte[] buf = new byte[1024];
				int len = in.read(buf);
				while (len != -1) {
					out.write(buf, 0, len);
					out.flush();
					len = in.read(buf);
				}
			}
			catch (Exception ex) {
				// just ignore it
			}
		}
	}

	static class ShutDownPythonHook extends Thread {
		private Process p;
		private String pyFileDir;

		public ShutDownPythonHook(Process p, String pyFileDir) {
			this.p = p;
			this.pyFileDir = pyFileDir;
		}

		public void run() {

			p.destroyForcibly();

			if (pyFileDir != null) {
				File pyDir = new File(pyFileDir);
				FileUtils.deleteDirectoryQuietly(pyDir);
			}
		}
	}

}
