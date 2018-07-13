package org.apache.flink.mesos.runtime.clusterframework;

import org.apache.flink.configuration.Configuration;

import org.junit.Test;

import java.util.Iterator;
import java.util.Set;

import static org.junit.Assert.assertEquals;

/**
 * Test that mesos config are extracted correctly from the configuration.
 */
public class LaunchableMesosWorkerTest {

	@Test
	public void canGetPortKeys() {
		// Setup
		Configuration config = new Configuration();
		config.setString(LaunchableMesosWorker.PORT_ASSIGNMENT_KEY, "someport.here,anotherport");

		// Act
		Set<String> portKeys = LaunchableMesosWorker.extractPortKeys(config);

		// Assert
		assertEquals("Must get right number of port keys", 4, portKeys.size());
		Iterator<String> iterator = portKeys.iterator();
		assertEquals("port key must be correct", LaunchableMesosWorker.TM_PORT_KEYS[0], iterator.next());
		assertEquals("port key must be correct", LaunchableMesosWorker.TM_PORT_KEYS[1], iterator.next());
		assertEquals("port key must be correct", "someport.here", iterator.next());
		assertEquals("port key must be correct", "anotherport", iterator.next());
	}

}
