package org.apache.flink.mesos.runtime.clusterframework;

/**
 * The Mesos environment variables used for settings of the containers.
 */
public class MesosConfigKeys {
	// ------------------------------------------------------------------------
	//  Environment variable names
	// ------------------------------------------------------------------------

	public static final String ENV_TM_MEMORY = "_CLIENT_TM_MEMORY";
	public static final String ENV_TM_COUNT = "_CLIENT_TM_COUNT";
	public static final String ENV_SLOTS = "_SLOTS";
	public static final String ENV_CLIENT_SHIP_FILES = "_CLIENT_SHIP_FILES";
	public static final String ENV_CLIENT_USERNAME = "_CLIENT_USERNAME";
	public static final String ENV_DYNAMIC_PROPERTIES = "_DYNAMIC_PROPERTIES";
	public static final String ENV_FLINK_CONTAINER_ID = "_FLINK_CONTAINER_ID";
	public static final String ENV_FLINK_TMP_DIR = "_FLINK_TMP_DIR";
	public static final String ENV_FLINK_CLASSPATH = "_FLINK_CLASSPATH";
	public static final String ENV_CLASSPATH = "CLASSPATH";
	public static final String ENV_MESOS_SANDBOX = "MESOS_SANDBOX";
	public static final String ENV_SESSION_ID = "_CLIENT_SESSION_ID";

	/** Private constructor to prevent instantiation */
	private MesosConfigKeys() {}
}
