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
package org.apache.flink.runtime.yarn;

import java.io.Serializable;


/**
 * Simple status representation of a running YARN cluster.
 * It contains the number of available Taskmanagers and processing slots.
 */
public class FlinkYarnClusterStatus implements Serializable {
	private static final long serialVersionUID = 4230348124179245370L;
	private int numberOfTaskManagers;
	private int numberOfSlots;

	public FlinkYarnClusterStatus() {
	}

	public FlinkYarnClusterStatus(int numberOfTaskManagers, int numberOfSlots) {
		this.numberOfTaskManagers = numberOfTaskManagers;
		this.numberOfSlots = numberOfSlots;
	}

	public int getNumberOfTaskManagers() {
		return numberOfTaskManagers;
	}

	public void setNumberOfTaskManagers(int numberOfTaskManagers) {
		this.numberOfTaskManagers = numberOfTaskManagers;
	}

	public int getNumberOfSlots() {
		return numberOfSlots;
	}

	public void setNumberOfSlots(int numberOfSlots) {
		this.numberOfSlots = numberOfSlots;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		FlinkYarnClusterStatus that = (FlinkYarnClusterStatus) o;

		if (numberOfSlots != that.numberOfSlots) {
			return false;
		}
		return numberOfTaskManagers == that.numberOfTaskManagers;

	}

	@Override
	public int hashCode() {
		int result = numberOfTaskManagers;
		result = 31 * result + numberOfSlots;
		return result;
	}

	@Override
	public String toString() {
		return "FlinkYarnClusterStatus{" +
				"numberOfTaskManagers=" + numberOfTaskManagers +
				", numberOfSlots=" + numberOfSlots +
				'}';
	}
}
