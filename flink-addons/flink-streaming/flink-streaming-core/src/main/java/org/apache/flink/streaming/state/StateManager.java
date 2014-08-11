/**
 *
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
 *
 */

package org.apache.flink.streaming.state;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedList;

public class StateManager implements Runnable, Serializable {

	private static final long serialVersionUID = 1L;
	private LinkedList<Object> stateList = new LinkedList<Object>();
	private long checkpointInterval;
	private String filename;

	public StateManager(String filename, long checkpointIntervalMS) {
		this.filename = filename;
		this.checkpointInterval = checkpointIntervalMS;
	}

	public void registerState(Object state) {
		stateList.add(state);
	}

	@SuppressWarnings("unused")
	public void restoreState() {
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(new FileInputStream(filename));
			for (Object state : stateList) {
				state = ois.readObject();
			}
			ois.close();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	// run checkpoint.
	@SuppressWarnings("resource")
	@Override
	public void run() {
		ObjectOutputStream oos = null;
		try {
			oos = new ObjectOutputStream(new FileOutputStream(filename));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		// take snapshot of every registered state.
		while (true) {
			try {
				Thread.sleep(checkpointInterval);
				for (Object state : stateList) {
					oos.writeObject(state);
					oos.flush();
				}
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
