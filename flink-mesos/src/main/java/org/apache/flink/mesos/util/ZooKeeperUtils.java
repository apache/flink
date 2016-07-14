package org.apache.flink.mesos.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.configuration.Configuration;

public class ZooKeeperUtils {

	/**
	 * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper
	 * quorum.
	 *
	 * @param configuration {@link Configuration} object containing the configuration values
	 * @return {@link CuratorFramework} instance
	 */
	@SuppressWarnings("unchecked")
	public static CuratorFramework startCuratorFramework(Configuration configuration) {

		// workaround for shaded curator dependency of flink-runtime
		Object client = org.apache.flink.runtime.util.ZooKeeperUtils.startCuratorFramework(configuration);
		return (CuratorFramework) client;
	}
}
