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

package eu.stratosphere.nephele.instance.ec2;

/**
 * This class provides auxiliary methods needed to set up the custom EC2-image.
 * 
 * @author casp
 */
final class EC2Utilities {

	static synchronized String createTaskManagerUserData(final String JobManagerIpAddress) {

		/*
		 * When the type is set to TASKMANAGER (in /tmp/STRATOSPHERE_TYPE
		 * then the AMI assumes that another file called /tmp/JOBMANAGER_ADDRESS
		 * exists containing the (internal) IP address of the jobmanager instance.
		 */

		final String taskManagerUserData = "#!/bin/bash \n echo TASKMANAGER >> /tmp/STRATOSPHERE_TYPE \n echo "
			+ JobManagerIpAddress + " >> /tmp/JOBMANAGER_ADDRESS";

		return new String(org.apache.commons.codec.binary.Base64.encodeBase64(taskManagerUserData.getBytes()));

	}
}
