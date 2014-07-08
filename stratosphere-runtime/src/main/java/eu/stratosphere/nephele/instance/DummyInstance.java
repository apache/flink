/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.nephele.instance;

/**
 * A DummyInstance is a stub implementation of the {@link Instance} interface.
 * Dummy instances are used to plan a job execution but must be replaced with
 * concrete instances before the job execution starts.
 * 
 */
public class DummyInstance extends Instance {

	private static int nextID = 0;

	private final String name;

	public static synchronized DummyInstance createDummyInstance() {

		return new DummyInstance(nextID++);
	}

	/**
	 * Constructs a new dummy instance of the given instance type.
	 * 
	 * @param id
	 *        the ID of the dummy instance
	 */
	private DummyInstance(int id) {
		super(null, null, null, null, 0);

		this.name = "DummyInstance_" + Integer.toString(id);
	}


	@Override
	public String toString() {

		return this.name;
	}


	@Override
	public HardwareDescription getHardwareDescription() {

		throw new RuntimeException("getHardwareDescription is called on a DummyInstance");
	}
}
