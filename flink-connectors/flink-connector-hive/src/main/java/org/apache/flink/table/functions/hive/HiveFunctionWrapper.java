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

package org.apache.flink.table.functions.hive;

import org.apache.flink.annotation.Internal;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.Serializable;

/**
 * A wrapper of Hive functions that instantiate function instances and ser/de functino instance cross process boundary.
 *
 * @param <UDFType> The type of UDF.
 */
@Internal
public class HiveFunctionWrapper<UDFType> implements Serializable {

	public static final long serialVersionUID = 393313529306818205L;

	private final String className;

	private transient UDFType instance = null;

	public HiveFunctionWrapper(String className) {
		this.className = className;
	}

	/**
	 * Instantiate a Hive function instance.
	 *
	 * @return a Hive function instance
	 */
	public UDFType createFunction() {
		if (instance != null) {
			return instance;
		} else {
			UDFType func = null;
			try {
				func = (UDFType) Thread.currentThread().getContextClassLoader().loadClass(className).newInstance();
			} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
				throw new FlinkHiveUDFException(
					String.format("Failed to create function from %s", className), e);
			}

			if (!(func instanceof UDF)) {
				// We cache the function if it is not the Simple UDF,
				// as we always have to create new instance for Simple UDF.
				instance = func;
			}

			return func;
		}
	}

	/**
	 * Get class name of the Hive function.
	 *
	 * @return class name of the Hive function
	 */
	public String getClassName() {
		return className;
	}

	/**
	 * Get class of the Hive function.
	 *
	 * @return class of the Hive function
	 * @throws ClassNotFoundException thrown when the class is not found in classpath
	 */
	public Class<UDFType> getUDFClass() throws ClassNotFoundException {
		return (Class<UDFType>) Class.forName(className);
	}
}

