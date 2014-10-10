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
package org.apache.flink.yarn;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.flink.runtime.ipc.RPC;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.yarn.rpc.ApplicationMasterStatus;
import org.apache.flink.yarn.rpc.YARNClientMasterProtocol;
import org.apache.flink.yarn.rpc.YARNClientMasterProtocol.Message;


public class ClientMasterControl extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(ClientMasterControl.class);

	private InetSocketAddress applicationMasterAddress;

	private ApplicationMasterStatus appMasterStatus;
	private YARNClientMasterProtocol cmp;
	private Object lock = new Object();
	private List<Message> messages = new ArrayList<Message>();
	private boolean running = true;

	public ClientMasterControl(InetSocketAddress applicationMasterAddress) {
		super();
		this.applicationMasterAddress = applicationMasterAddress;
	}

	@Override
	public void run() {
		try {
			cmp = RPC.getProxy(YARNClientMasterProtocol.class, applicationMasterAddress, NetUtils.getSocketFactory());

			while(running) {
				synchronized (lock) {
					try {
						appMasterStatus = cmp.getAppplicationMasterStatus();
					} catch(Throwable e) {
						// TODO: try to clean up as much as possible! (set to failed state? // kill app? // clean up files)
						LOG.warn("Failed to get Application Master status", e);
					}
					if(appMasterStatus != null && messages.size() != appMasterStatus.getMessageCount()) {
						messages = cmp.getMessages();
					}
				}

				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					LOG.warn("Error while getting application status", e);
				}
			}
			RPC.stopProxy(cmp);
		} catch (IOException e) {
			LOG.warn("Error while running RPC service", e);
		}

	}

	public int getNumberOfTaskManagers() {
		synchronized (lock) {
			if(appMasterStatus == null) {
				return 0;
			}
			return appMasterStatus.getNumberOfTaskManagers();
		}
	}

	public int getNumberOfAvailableSlots() {
		synchronized (lock) {
			if(appMasterStatus == null) {
				return 0;
			}
			return appMasterStatus.getNumberOfAvailableSlots();
		}
	}
	
	public boolean getFailedStatus() {
		synchronized (lock) {
			if(appMasterStatus == null) {
				return false;
			}
			return appMasterStatus.getFailed();
		}
	}

	public void shutdownAM() {
		try {
			cmp.shutdownAM();
		} catch(Throwable e) {
			// the old RPC service is unable to shut down itself. So the java.io.EOFException is expected here.
			LOG.debug("This exception is expected", e);
		}
	}

	public List<Message> getMessages() {
		return this.messages;
	}

	public void close() {
		running = false;
	}

	public void addTaskManagers(int n) {
		cmp.addTaskManagers(n);
	}
}
