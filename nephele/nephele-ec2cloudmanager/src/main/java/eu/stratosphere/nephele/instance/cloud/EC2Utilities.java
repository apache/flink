package eu.stratosphere.nephele.instance.cloud;

/**
 * This class provides auxiliary methods needed to set up the custom EC2-image.
 * @author casp
 *
 */
public class EC2Utilities {

	
    public static synchronized String createTaskManagerUserData(String JobManagerIpAddress) {

        /*
         * When the type is set to TASKMANAGER (in /tmp/STRATOSPHERE_TYPE
         * then the AMI assumes that another file called /tmp/JOBMANAGER_ADDRESS
         * exists containing the (internal) IP address of the jobmanager instance.
         */

        final String taskManagerUserData = "#!/bin/bash \n echo TASKMANAGER >> /tmp/STRATOSPHERE_TYPE \n echo " + JobManagerIpAddress + " >> /tmp/JOBMANAGER_ADDRESS";

        return new String(org.apache.commons.codec.binary.Base64.encodeBase64(taskManagerUserData.getBytes()));

    }
}
