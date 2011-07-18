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

package eu.stratosphere.nephele.instance.cloud;

import java.util.Hashtable;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;

/**
 * This class is managing the EC2 clients.
 * @author casp
 *
 */
public class EC2ClientFactory {

	//Already created EC2 clients are stored in this hashtable.
	//This makes it possible to use multiple EC2 credentials.
	private static Hashtable<String, AmazonEC2Client> ec2clients = new Hashtable<String, AmazonEC2Client>();
	
	/**
	 * This factory method returns a the corresponding EC2Client object for the given credentials.
	 * @param awsAccessId
	 * @param awsSecretKey
	 * @return
	 */
	static synchronized AmazonEC2Client getEC2Client(String awsAccessId, String awsSecretKey){

		
		//Check if a client-object was already generated
		if(ec2clients.containsKey(awsAccessId)){
			return ec2clients.get(awsAccessId);
		}
		
		//Create new EC2Client with given credentials
		
		BasicAWSCredentials credentials = new BasicAWSCredentials(awsAccessId, awsSecretKey);
		AmazonEC2Client client = new AmazonEC2Client(credentials);
		
		//TODO: Make endpoints configurable (US, EU, Asia etc).
		//client.setEndpoint(arg0)
		ec2clients.put(awsAccessId, client);
		return client;
	}
	
}
