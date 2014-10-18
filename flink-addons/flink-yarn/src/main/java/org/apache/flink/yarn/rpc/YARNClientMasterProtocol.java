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

package org.apache.flink.yarn.rpc;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.protocols.VersionedProtocol;


/**
 * Interface describing the methods offered by the RPC service between
 * the Client and Application Master
 */
public interface YARNClientMasterProtocol extends VersionedProtocol {

	/**
	 * Message from Am to Client.
	 *
	 */
	public static class Message implements IOReadableWritable {
		private String text;
		private Date date;
		
		public Message() {	
			// for deserializability
		}
		
		public Message(String msg) {
			this.text = msg;
			this.date = new Date();
		}
		
		public String getMessage() {
			return "["+date+"] "+text;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeUTF(text);
			out.writeLong(date.getTime());
		}

		@Override
		public void read(DataInputView in) throws IOException {
			text = in.readUTF();
			date = new Date(in.readLong());
		}
	}

	ApplicationMasterStatus getAppplicationMasterStatus();

	void shutdownAM() throws Exception;

	List<Message> getMessages();

	void addTaskManagers(int n);
}
