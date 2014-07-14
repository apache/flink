/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.state.manager;

import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedList;

public class StateCheckpointer implements Runnable, Serializable {

	private LinkedList<Object> stateList = new LinkedList<Object>();
	ObjectOutputStream oos;
	long timeInterval;

	public StateCheckpointer(String filename, long timeIntervalMS) {
		try {
			oos = new ObjectOutputStream(new FileOutputStream(filename));
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.timeInterval = timeIntervalMS;
	}

	public void RegisterState(Object state) {
		stateList.add(state);
	}

	@Override
	public void run() {
		// take snapshot of every registered state.
		while (true) {
			try {
				Thread.sleep(timeInterval);
				for (Object state : stateList) {
					oos.writeObject(state);
					oos.flush();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
}
