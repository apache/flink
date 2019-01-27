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

package org.apache.flink.table.hive.functions;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.Serializable;
import java.util.List;

/**
 * This class provides the UDF creation and also the UDF instance serialization and
 * de-serialization cross process boundary.
 *
 * @param <UDFType> The type of UDF.
 */
public class HiveFunctionWrapper<UDFType> implements Serializable {

	private String className;
	private List<Boolean> constants;

	private transient UDFType instance = null;

	public HiveFunctionWrapper(String className) {
		this.className = className;
	}

	public HiveFunctionWrapper(Class<?> clazz) {
		this.className = clazz.getName();
	}

	public UDFType createFunction()
			throws ClassNotFoundException, IllegalAccessException, InstantiationException {
		if (null != instance) {
			return instance;
		}
		UDFType func =  (UDFType) getClassLoader().loadClass(className).newInstance();
		if (!(func instanceof UDF)) {
			// We cache the function if it is not the Simple UDF,
			// as we always have to create new instance for Simple UDF.
			instance = func;
		}
		return func;
	}

	public Class<UDFType> getUDFClass() throws ClassNotFoundException {
		return (Class<UDFType>) Class.forName(className);
	}

	public static ClassLoader getClassLoader() {
		return Thread.currentThread().getContextClassLoader();
	}

	public void setConstants(List<Boolean> constants) {
		this.constants = constants;
	}

	public List<Boolean> getConstants() {
		return constants;
	}
}
