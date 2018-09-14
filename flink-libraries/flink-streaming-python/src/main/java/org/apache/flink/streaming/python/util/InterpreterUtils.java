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

package org.apache.flink.streaming.python.util;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.python.api.environment.PythonConstants;
import org.apache.flink.streaming.python.api.environment.PythonEnvironmentFactory;
import org.apache.flink.streaming.python.api.environment.PythonStreamExecutionEnvironment;
import org.apache.flink.streaming.python.util.serialization.SerializationUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;

import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * A collection of utility methods for interacting with jython.
 *
 * <p><strong>Important:</strong> This class loads the jython runtime which is essentially one big singleton. We make
 * the core assumption here that this class is loaded by separate ClassLoaders for each job or task allowing multiple
 * instances of jython to exist in the same JVM without affecting each other.
 */
public class InterpreterUtils {
	private static final Logger LOG = LoggerFactory.getLogger(InterpreterUtils.class);

	private static PythonInterpreter pythonInterpreter = null;
	private static boolean jythonInitialized = false;

	private InterpreterUtils() {
	}

	/**
	 * Deserialize the given python function. If the functions class definition cannot be found we assume that this is
	 * the first invocation of this method for a given job and load the python script containing the class definition
	 * via jython.
	 *
	 * @param context the RuntimeContext of the java function
	 * @param serFun serialized python UDF
	 * @return deserialized python UDF
	 * @throws FlinkException if the deserialization failed
	 */
	@SuppressWarnings("unchecked")
	public static <X> X deserializeFunction(RuntimeContext context, byte[] serFun) throws FlinkException {
		if (!jythonInitialized) {
			// This branch is only tested by end-to-end tests
			String path = context.getDistributedCache().getFile(PythonConstants.FLINK_PYTHON_DC_ID).getAbsolutePath();

			String scriptName = PythonStreamExecutionEnvironment.PythonJobParameters.getScriptName(context.getExecutionConfig().getGlobalJobParameters());

			try {
				initPythonInterpreter(
					new String[]{Paths.get(path, scriptName).toString()},
					path,
					scriptName);
			} catch (Exception e) {
				LOG.error("Initialization of jython failed.");
				try {
					LOG.error("Initialization of jython failed.", e);
					throw new FlinkRuntimeException("Initialization of jython failed.", e);
				} catch (Exception ie) {
					// this may occur if the initial exception relies on jython being initialized properly
					LOG.error("Initialization of jython failed. Could not print original stacktrace.", ie);
					throw new FlinkRuntimeException("Initialization of jython failed. Could not print original stacktrace.");
				}
			}
		}

		try {
			return (X) SerializationUtils.deserializeObject(serFun);
		} catch (IOException | ClassNotFoundException ex) {
			throw new FlinkException("Deserialization of user-function failed.", ex);
		}
	}

	/**
	 * Initializes the Jython interpreter and executes a python script.
	 *
	 * @param factory environment factory
	 * @param scriptDirectory the directory containing all required user python scripts
	 * @param scriptName the name of the main python script
	 * @param args Command line arguments that will be delivered to the executed python script
	 */
	public static void initAndExecPythonScript(PythonEnvironmentFactory factory, java.nio.file.Path scriptDirectory, String scriptName, String[] args) {
		String[] fullArgs = new String[args.length + 1];
		fullArgs[0] = scriptDirectory.resolve(scriptName).toString();
		System.arraycopy(args, 0, fullArgs, 1, args.length);

		PythonInterpreter pythonInterpreter = initPythonInterpreter(fullArgs, scriptDirectory.toUri().getPath(), scriptName);

		pythonInterpreter.set("__flink_env_factory__", factory);
		pythonInterpreter.exec(scriptName + ".main(__flink_env_factory__)");
	}

	private static synchronized PythonInterpreter initPythonInterpreter(String[] args, String pythonPath, String scriptName) {
		if (!jythonInitialized) {
			// the java stack traces within the jython runtime aren't useful for users
			System.getProperties().put("python.options.includeJavaStackInExceptions", "false");
			PySystemState.initialize(System.getProperties(), new Properties(), args);

			pythonInterpreter = new PythonInterpreter();

			pythonInterpreter.getSystemState().path.add(0, pythonPath);

			pythonInterpreter.setErr(System.err);
			pythonInterpreter.setOut(System.out);

			pythonInterpreter.exec("import " + scriptName);
			jythonInitialized = true;
		}
		return pythonInterpreter;
	}
}
