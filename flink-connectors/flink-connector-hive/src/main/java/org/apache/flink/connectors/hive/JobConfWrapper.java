/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.hive;

import org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

/**
 * Wrap {@link JobConf} to a serializable class.
 */
public class JobConfWrapper implements Serializable {

	private static final long serialVersionUID = 1L;

	private transient JobConf jobConf;

	public JobConfWrapper(JobConf jobConf) {
		this.jobConf = jobConf;
	}

	public JobConf conf() {
		return jobConf;
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		out.defaultWriteObject();

		// we write the jobConf through a separate serializer to avoid cryptic exceptions when it
		// corrupts the serialization stream
		final DataOutputSerializer ser = new DataOutputSerializer(256);
		jobConf.write(ser);
		out.writeInt(ser.length());
		out.write(ser.getSharedBuffer(), 0, ser.length());
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		final byte[] data = new byte[in.readInt()];
		in.readFully(data);
		final DataInputDeserializer deser = new DataInputDeserializer(data);
		this.jobConf = new JobConf();
		try {
			jobConf.readFields(deser);
		} catch (IOException e) {
			throw new IOException(
					"Could not deserialize JobConf, the serialized and de-serialized don't match.", e);
		}
		Credentials currentUserCreds = HadoopInputFormatCommonBase.getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
		if (currentUserCreds != null) {
			jobConf.getCredentials().addAll(currentUserCreds);
		}
	}
}
