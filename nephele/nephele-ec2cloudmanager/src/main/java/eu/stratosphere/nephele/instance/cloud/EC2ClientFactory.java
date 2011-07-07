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
	static AmazonEC2Client getEC2Client(String awsAccessId, String awsSecretKey){
		
		//Check if a client-object was already generated
		if(ec2clients.containsKey(awsAccessId)){
			return ec2clients.get(awsAccessId);
		}
		
		//Create new EC2Client with given credentials
		
		BasicAWSCredentials credentials = new BasicAWSCredentials(awsAccessId, awsSecretKey);
		AmazonEC2Client client = new AmazonEC2Client(credentials);
		
		ec2clients.put(awsAccessId, client);
		return client;
	}
	
}
