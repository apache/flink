/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package org.apache.flink.runtime.jobmanager.scheduler;

import java.util.Comparator;

import org.apache.flink.runtime.instance.Instance;

public class InstanceFillDegreeComparator implements Comparator<Instance> {

	@Override
	public int compare(Instance o1, Instance o2) {
		float fraction1 = o1.getNumberOfAvailableSlots() / (float) o1.getTotalNumberOfSlots();
		float fraction2 = o2.getNumberOfAvailableSlots() / (float) o2.getTotalNumberOfSlots();
		
		return fraction1 < fraction2 ? -1 : 1;
	}
}
