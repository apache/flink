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
package org.apache.flink.streaming.python.api.functions;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.python.api.environment.PythonEnvironmentConfig;
import org.apache.flink.streaming.python.util.serialization.SerializationUtils;
import org.python.core.Py;
import org.python.core.PyObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.python.core.PySystemState;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * A collection of utility functions that are used by the python wrappers thin layer over
 * the streaming functions.
 */
public class UtilityFunctions {
	private static boolean jythonInitialized = false;
	private static PythonInterpreter pythonInterpreter;
	private static final Logger LOG = LoggerFactory.getLogger(UtilityFunctions.class);

	private UtilityFunctions() {
	}

	/**
	 * A generic map operator that convert any java type to PyObject. It is mainly used to convert elements
	 * collected from a source functions, to PyObject objects.
	 *
	 * @param <IN> Any given java object
	 */
	public static class SerializerMap<IN> implements MapFunction<IN, PyObject> {
		private static final long serialVersionUID = 1582769662549499373L;

		@Override
		public PyObject map(IN value) throws Exception {
			return UtilityFunctions.adapt(value);
		}
	}

	/**
	 * Convert java object to its corresponding PyObject representation.
	 *
	 * @param o Java object
	 * @return PyObject
	 */
	public static PyObject adapt(Object o) {
		if (o instanceof PyObject) {
			return (PyObject)o;
		}
		return  Py.java2py(o);
	}

	/**
	 * Deserializes Python UDF functions in a "smart" way. It first tries to extract the function using a common
	 * java method to deserialize an object. If it fails, it is assumed that the function object definition does not
	 * exist, and therefore, a Jython interpreter is initialized and the relevant python script is executed (without
	 * actually executing the 'execute' function). This result from this operation is that all the Python UDF objects
	 * that are defined in that python script are also defined in the local JVM. Is is now possible to extract
	 * the function using a common java method to deserialize the java function.
	 *
	 * @param runtimeCtx The runtime context of the executed job.
	 * @param serFun A serialized form of the given Python UDF
	 * @return An extracted java function
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static synchronized Object smartFunctionDeserialization(RuntimeContext runtimeCtx, byte[] serFun) throws IOException, ClassNotFoundException, InterruptedException {
		try {
			return SerializationUtils.deserializeObject(serFun);
		} catch (Exception e) {
			String path = runtimeCtx.getDistributedCache().getFile(PythonEnvironmentConfig.FLINK_PYTHON_DC_ID).getAbsolutePath();

			initPythonInterpreter(path, new String[]{""});

			PySystemState pySysStat = Py.getSystemState();
			pySysStat.path.add(0, path);

			String scriptFullPath = path + File.separator + PythonEnvironmentConfig.FLINK_PYTHON_PLAN_NAME;
			LOG.debug("Execute python script, path=" + scriptFullPath);
			pythonInterpreter.execfile(scriptFullPath);

			pySysStat.path.remove(path);

			return SerializationUtils.deserializeObject(serFun);
		}
	}

	/**
	 * Initializes the Jython interpreter and executes a python script.
	 *
	 * @param scriptFullPath The script full path
	 * @param args Command line arguments that will be delivered to the executed python script
	 * @throws IOException
	 */
	public static void initAndExecPythonScript(File scriptFullPath, String[] args) throws IOException {
		initPythonInterpreter(scriptFullPath.getParent(), args);
		pythonInterpreter.execfile(scriptFullPath.getAbsolutePath());

		LOG.debug("Cleaning up temporary folder: " + scriptFullPath.getParent());
		FileSystem fs = FileSystem.get(scriptFullPath.toURI());
		fs.delete(new Path(scriptFullPath.getParent()), true);
	}

	private static synchronized void initPythonInterpreter(String pythonPath, String[] args) {
		if (!jythonInitialized) {
			LOG.debug("Init python interpreter, path=" + pythonPath);
			Properties postProperties = new Properties();
			postProperties.put("python.path", pythonPath);
			PythonInterpreter.initialize(System.getProperties(), postProperties, args);

			pythonInterpreter = new PythonInterpreter();
			pythonInterpreter.setErr(System.err);
			pythonInterpreter.setOut(System.out);

			jythonInitialized = true;
		}
	}
}
