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

import java.util.Hashtable;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;

import eu.stratosphere.nephele.configuration.GlobalConfiguration;

/**
 * This class is managing the EC2 clients.
 * 
 * @author casp
 */
final class EC2ClientFactory {

	// Already created EC2 clients are stored in this hashtable.
	// This makes it possible to use multiple EC2 credentials.
	private static Hashtable<String, AmazonEC2Client> ec2clients = new Hashtable<String, AmazonEC2Client>();

	/**
	 * This factory method returns a the corresponding EC2Client object for the given credentials.
	 * 
	 * @param awsAccessId
	 * @param awsSecretKey
	 * @return the desired AmazonEC2Client.
	 */
	static synchronized AmazonEC2Client getEC2Client(final String awsAccessId, final String awsSecretKey) {

		if(awsAccessId == null) {
			throw new IllegalArgumentException("AWS access ID is null");
		}
		
		if(awsSecretKey == null) {
			throw new IllegalArgumentException("AWS secret key is null");
		}
		
		// Check if a client-object was already generated
		if (ec2clients.containsKey(awsAccessId)) {
			return ec2clients.get(awsAccessId);
		}

		// Create new EC2Client with given credentials

		final BasicAWSCredentials credentials = new BasicAWSCredentials(awsAccessId, awsSecretKey);
		final AmazonEC2Client client = new AmazonEC2Client(credentials);

		final String endpoint = GlobalConfiguration.getString("instancemanager.ec2.endpoint", "ec2.eu-west-1.amazonaws.com");

		client.setEndpoint(endpoint);

		ec2clients.put(awsAccessId, client);
		return client;
	}

}
