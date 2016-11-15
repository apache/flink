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

package org.apache.flink.streaming.python.api.datastream;

import org.python.util.PythonObjectInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectStreamClass;

/**
 * A helper class to overcome the inability to set the serialVersionUID in a python user-defined
 * function (UDF).
 * <p>The fact the this field is not set, results in a dynamic calculation of this serialVersionUID,
 * using SHA, to make sure it is a unique number. This unique number is a 64-bit hash of the
 * class name, interface class names, methods, and fields. If a Python class inherits from a
 * Java class, as in the case of Python UDFs, then a proxy wrapper class is created. Its name is
 * constructed using the following pattern:
 * <b>{@code org.python.proxies.<module-name>$<UDF-class-name>$<number>}</b>. The {@code <number>}
 * part is increased by one in runtime, for every job submission. It results in different serial
 * version UID for each run for the same Python class. Therefore, it is required to silently
 * suppress the serial version UID mismatch check.</p>
 */
public class PythonObjectInputStream2 extends PythonObjectInputStream {

	public PythonObjectInputStream2(InputStream in) throws IOException {
		super(in);
	}

	protected ObjectStreamClass readClassDescriptor() throws IOException, ClassNotFoundException {
		ObjectStreamClass resultClassDescriptor = super.readClassDescriptor(); // initially streams descriptor

		Class<?> localClass;
		try {
			localClass = resolveClass(resultClassDescriptor);
		} catch (ClassNotFoundException e) {
			System.out.println("No local class for " + resultClassDescriptor.getName());
			return resultClassDescriptor;
		}

		ObjectStreamClass localClassDescriptor = ObjectStreamClass.lookup(localClass);
		if (localClassDescriptor != null) { // only if class implements serializable
			final long localSUID = localClassDescriptor.getSerialVersionUID();
			final long streamSUID = resultClassDescriptor.getSerialVersionUID();
			if (streamSUID != localSUID) { // check for serialVersionUID mismatch.
				// Overriding serialized class version mismatch
				resultClassDescriptor = localClassDescriptor; // Use local class descriptor for deserialization
			}
		}
		return resultClassDescriptor;
	}
}
