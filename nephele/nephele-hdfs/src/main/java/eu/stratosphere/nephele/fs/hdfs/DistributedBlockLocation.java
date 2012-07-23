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

package eu.stratosphere.nephele.fs.hdfs;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.nephele.fs.BlockLocation;

/**
 * Implementation of the {@link BlockLocation} interface for the
 * Hadoop Distributed File System.
 * 
 * @author warneke
 */
public final class DistributedBlockLocation implements BlockLocation {

	/**
	 * Specifies the character separating the hostname from the domain name.
	 */
	private static final char DOMAIN_SEPARATOR = '.';

	/**
	 * Regular expression for an IPv4 address.
	 */
	private static final Pattern IPV4_PATTERN = Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");

	/**
	 * The original Hadoop block location object.
	 */
	private final org.apache.hadoop.fs.BlockLocation blockLocation;

	/**
	 * Stores the hostnames without the domain suffix.
	 */
	private String[] hostnames;

	/**
	 * Creates a new block location
	 * 
	 * @param blockLocation
	 *        the original HDFS block location
	 */
	public DistributedBlockLocation(final org.apache.hadoop.fs.BlockLocation blockLocation) {

		this.blockLocation = blockLocation;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String[] getHosts() throws IOException {

		/**
		 * Unfortunately, the Hadoop API is not precise about if the list returned by BlockLocation.getHosts() contains
		 * the hostnames with their respective domain suffix or not (FQDN or not). We have witnessed both versions,
		 * depending on the cluster's network configuration. As a workaround, we therefore strip every hostname to make
		 * sure it does not contain the domain suffix.
		 */
		if (this.hostnames == null) {

			final String[] hadoopHostnames = blockLocation.getHosts();
			this.hostnames = new String[hadoopHostnames.length];

			for (int i = 0; i < hadoopHostnames.length; ++i) {
				this.hostnames[i] = stripHostname(hadoopHostnames[i]);
			}
		}

		return this.hostnames;
	}

	/**
	 * Looks for a domain suffix in a FQDN and strips it if present.
	 * 
	 * @param originalHostname
	 *        the original hostname, possibly an FQDN
	 * @return the stripped hostname without the domain suffix
	 */
	private static String stripHostname(final String originalHostname) {

		// Check if the hostname domains the domain separator character
		final int index = originalHostname.indexOf(DOMAIN_SEPARATOR);
		if (index == -1) {
			return originalHostname;
		}

		// Make sure we are not stripping an IPv4 address
		final Matcher matcher = IPV4_PATTERN.matcher(originalHostname);
		if (matcher.matches()) {
			return originalHostname;
		}

		if (index == 0) {
			throw new IllegalStateException("Hostname " + originalHostname + " starts with a " + DOMAIN_SEPARATOR);
		}

		return originalHostname.substring(0, index);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getLength() {

		return this.blockLocation.getLength();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getOffset() {

		return this.blockLocation.getOffset();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int compareTo(final BlockLocation o) {

		final long diff = getOffset() - o.getOffset();

		return diff < 0 ? -1 : diff > 0 ? 1 : 0;
	}
}
