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

import org.apache.flink.table.api.functions.FunctionContext;
import org.apache.flink.table.api.functions.ScalarFunction;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.errorcode.TableErrors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * Python scalar UDF wrapper.
 */
public class PythonScalarFunction extends ScalarFunction {
	private static final Logger LOG = LoggerFactory.getLogger(PythonScalarFunction.class);

	private String moduleName;
	private String funcName;
	private InternalType returnType;

	private Socket worker;
	private final int bufferSize = 8192;

	public PythonScalarFunction(String funcName, String moduleName, InternalType returnType) {
		this.moduleName = moduleName;
		this.funcName  = funcName;
		this.returnType = returnType;
	}

	@Override
	public void open(FunctionContext context) {
		worker = PythonUDFUtil.createWorkerSocket(context);
	}

	public Object eval(Object... args) {
		try {
			DataOutputStream out = new DataOutputStream(
				new BufferedOutputStream(worker.getOutputStream(), bufferSize));

			PythonUDFUtil.sendCallRequest(moduleName, out, args);

			DataInputStream in = new DataInputStream(
				new BufferedInputStream(worker.getInputStream()));

			// header
			int protocol = in.readInt();
			assert(protocol == PythonUDFUtil.PROTOCAL_VER);
			int ctrlFlag = in.readInt();

			Object res = PythonUDFUtil.getResult(in);

			if (ctrlFlag == PythonUDFUtil.SCALAR_UDF_RESULT) {
				return res;
			}
			else if (ctrlFlag == PythonUDFUtil.PYTHON_EXCEPTION_THROWN){
				String err = res.toString();
				LOG.error(err);
				throw new RuntimeException(
					TableErrors.INST.sqlPythonUDFRunTimeError(funcName, moduleName, err));
			}
		}
		catch (IOException ioe) {
			LOG.error(ioe.getMessage());
			throw new RuntimeException(
				TableErrors.INST.sqlPythonUDFSocketIOError(funcName, moduleName, ioe.getMessage()));
		}

		return null;
	}

	@Override
	public void close() {
		if (worker != null) {
			try {
				worker.close();
			} catch (IOException e) {
				LOG.warn(e.getMessage());
			}
		}
	}

	@Override
	public DataType getResultType(Object[] arguments, Class[] argTypes) {
		return returnType;
	}

	public String toString() {
		return moduleName;
	}
}
