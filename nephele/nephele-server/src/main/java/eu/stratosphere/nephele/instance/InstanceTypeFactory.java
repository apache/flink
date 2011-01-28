/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.instance;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This factory constructs {@link InstanceType} objects.
 * 
 * @author warneke
 */
public class InstanceTypeFactory {

	/**
	 * The logger used to report errors.
	 */
	private static final Log LOG = LogFactory.getLog(InstanceTypeFactory.class);

	/**
	 * The pattern used to parse the hardware descriptions of instance types.
	 */
	private static Pattern INSTANCE_TYPE_PATTERN = Pattern.compile("^([^,]+),(\\d+),(\\d+),(\\d+),(\\d+),(\\d+)$");

	/**
	 * Private constructor, so class cannot be instantiated.
	 */
	private InstanceTypeFactory() {
	}

	/**
	 * Constructs an {@link InstanceType} object by parsing a hardware description string.
	 * 
	 * @param description
	 *        the hardware description reflected by this instance type
	 * @return an instance type reflecting the given hardware description or <code>null</code> if the description cannot
	 *         be parsed
	 */
	public static InstanceType constructFromDescription(String description) {

		final Matcher m = INSTANCE_TYPE_PATTERN.matcher(description);
		if (!m.matches()) {
			LOG.error("Cannot extract instance type from string " + description);
			return null;
		}

		final String identifier = m.group(1);
		final int numComputeUnits = Integer.parseInt(m.group(2));
		final int numCores = Integer.parseInt(m.group(3));
		final int memorySize = Integer.parseInt(m.group(4));
		final int diskCapacity = Integer.parseInt(m.group(5));
		final int pricePerHour = Integer.parseInt(m.group(6));

		return new InstanceType(identifier, numComputeUnits, numCores, memorySize, diskCapacity, pricePerHour);
	}

	/**
	 * Constructs an {@link InstanceType} from the given parameters.
	 * 
	 * @param identifier
	 *        identifier for this instance type
	 * @param numberOfComputeUnits
	 *        number of computational units of this instance type
	 * @param numberOfCores
	 *        number of CPU cores of this instance type
	 * @param memorySize
	 *        amount of main memory of this instance type (in MB)
	 * @param diskCapacity
	 *        disk capacity of this instance type (in GB)
	 * @param pricePerHour
	 *        price per hour that is charged for running instances of this type
	 */
	public static InstanceType construct(String identifier, int numberOfComputeUnits, int numberOfCores,
			int memorySize, int diskCapacity, int pricePerHour) {

		return new InstanceType(identifier, numberOfComputeUnits, numberOfCores, memorySize, diskCapacity, pricePerHour);
	}
}
