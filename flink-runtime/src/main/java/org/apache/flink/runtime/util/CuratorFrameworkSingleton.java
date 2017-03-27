package org.apache.flink.runtime.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.ACLProvider;
import org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;

public class CuratorFrameworkSingleton {
	private static final Logger LOG = LoggerFactory.getLogger(CuratorFrameworkSingleton.class);

	private volatile static CuratorFramework client = null;

	private CuratorFrameworkSingleton(Configuration configuration) {
		String zkQuorum = configuration.getValue(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM);

		if (zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
			throw new RuntimeException("No valid ZooKeeper quorum has been specified. " +
				"You can specify the quorum via the configuration key '" +
				HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key() + "'.");
		}

		int sessionTimeout = configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT);

		int connectionTimeout = configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_CONNECTION_TIMEOUT);

		int retryWait = configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_RETRY_WAIT);

		int maxRetryAttempts = configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_MAX_RETRY_ATTEMPTS);

		String root = configuration.getValue(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT);

		String namespace = configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);

		boolean disableSaslClient = configuration.getBoolean(ConfigConstants.ZOOKEEPER_SASL_DISABLE,
			ConfigConstants.DEFAULT_ZOOKEEPER_SASL_DISABLE);

		ACLProvider aclProvider;

		ZooKeeperUtils.ZkClientACLMode aclMode = ZooKeeperUtils.ZkClientACLMode.fromConfig(configuration);

		if(disableSaslClient && aclMode == ZooKeeperUtils.ZkClientACLMode.CREATOR) {
			String errorMessage = "Cannot set ACL role to " + aclMode +"  since SASL authentication is " +
				"disabled through the " + ConfigConstants.ZOOKEEPER_SASL_DISABLE + " property";
			LOG.warn(errorMessage);
			throw new IllegalConfigurationException(errorMessage);
		}

		if(aclMode == ZooKeeperUtils.ZkClientACLMode.CREATOR) {
			LOG.info("Enforcing creator for ZK connections");
			aclProvider = new ZooKeeperUtils.SecureAclProvider();
		} else {
			LOG.info("Enforcing default ACL for ZK connections");
			aclProvider = new DefaultACLProvider();
		}


		String rootWithNamespace = ZooKeeperUtils.generateZookeeperPath(root, namespace);

		LOG.info("Using '{}' as Zookeeper namespace.", rootWithNamespace);

		client = CuratorFrameworkFactory.builder()
			.connectString(zkQuorum)
			.sessionTimeoutMs(sessionTimeout)
			.connectionTimeoutMs(connectionTimeout)
			.retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
				// Curator prepends a '/' manually and throws an Exception if the
				// namespace starts with a '/'.
			.namespace(rootWithNamespace.startsWith("/") ? rootWithNamespace.substring(1) : rootWithNamespace)
			.aclProvider(aclProvider)
			.build();

		client.start();
	}

	public static synchronized CuratorFramework newClient(Configuration configuration) {
		if (client == null) {
			new CuratorFrameworkSingleton(configuration);
		}
		return client;
	}
}
