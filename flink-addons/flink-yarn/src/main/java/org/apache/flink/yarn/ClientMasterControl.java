/**
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.runtime.ipc.RPC;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.yarn.rpc.ApplicationMasterStatus;
import org.apache.flink.yarn.rpc.YARNClientMasterProtocol;
import org.apache.flink.yarn.rpc.YARNClientMasterProtocol.Message;


public class ClientMasterControl extends Thread {
	private static final Log LOG = LogFactory.getLog(ClientMasterControl.class);

	private InetSocketAddress applicationMasterAddress;

	private ApplicationMasterStatus appMasterStatus;
	private YARNClientMasterProtocol cmp;
	private Object lock = new Object();
	private List<Message> messages;
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
					appMasterStatus = cmp.getAppplicationMasterStatus();
					if(messages != null && appMasterStatus != null &&
							messages.size() != appMasterStatus.getMessageCount()) {
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

	public boolean shutdownAM() {
		try {
			return cmp.shutdownAM().getValue();
		} catch(Throwable e) {
			LOG.warn("Error shutting down the application master", e);
			return false;
		}
	}

	public List<Message> getMessages() {
		if(this.messages == null) {
			return new ArrayList<Message>();
		}
		return this.messages;
	}

	public void close() {
		running = false;
	}

	public void addTaskManagers(int n) {
		cmp.addTaskManagers(n);
	}
}
