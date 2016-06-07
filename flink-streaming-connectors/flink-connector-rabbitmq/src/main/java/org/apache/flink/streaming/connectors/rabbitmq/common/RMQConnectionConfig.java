/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.rabbitmq.common;

import com.rabbitmq.client.ConnectionFactory;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * Connection Configuration for RMQ.
 * If {@link Builder#setUri(String)} has been set then {@link RMQConnectionConfig#RMQConnectionConfig(String, Integer,
 * Boolean, Boolean, Integer, Integer, Integer, Integer)}
 * will be used for initialize the RMQ connection or
 * {@link RMQConnectionConfig#RMQConnectionConfig(String, Integer, String, String, String, Integer, Boolean,
 * Boolean, Integer, Integer, Integer, Integer)}
 * will be used for initialize the RMQ connection
 */
public class RMQConnectionConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(RMQConnectionConfig.class);

	private String host;
	private Integer port;
	private String virtualHost;
	private String username;
	private String password;
	private String uri;

	private Integer networkRecoveryInterval;
	private Boolean automaticRecovery;
	private Boolean topologyRecovery;

	private Integer connectionTimeout;
	private Integer requestedChannelMax;
	private Integer requestedFrameMax;
	private Integer requestedHeartbeat;

	/**
	*
	* @param host host name
	* @param port port
	* @param virtualHost virtual host
	* @param username username
	* @param password password
	* @param networkRecoveryInterval connection recovery interval in milliseconds
	* @param automaticRecovery if automatic connection recovery
	* @param topologyRecovery if topology recovery
	* @param connectionTimeout connection timeout
	* @param requestedChannelMax requested maximum channel number
	* @param requestedFrameMax requested maximum frame size
	* @param requestedHeartbeat requested heartbeat interval
	* @throws NullPointerException if host or virtual host or username or password is null
    */
	private RMQConnectionConfig(String host, Integer port, String virtualHost, String username, String password,
								Integer networkRecoveryInterval, Boolean automaticRecovery,
								Boolean topologyRecovery, Integer connectionTimeout, Integer requestedChannelMax,
								Integer requestedFrameMax, Integer requestedHeartbeat){
		Preconditions.checkNotNull(host, "host can not be null");
		Preconditions.checkNotNull(port, "port can not be null");
		Preconditions.checkNotNull(virtualHost, "virtualHost can not be null");
		Preconditions.checkNotNull(username, "username can not be null");
		Preconditions.checkNotNull(password, "password can not be null");
		this.host = host;
		this.port = port;
		this.virtualHost = virtualHost;
		this.username = username;
		this.password = password;

		this.networkRecoveryInterval = networkRecoveryInterval;
		this.automaticRecovery = automaticRecovery;
		this.topologyRecovery = topologyRecovery;
		this.connectionTimeout = connectionTimeout;
		this.requestedChannelMax = requestedChannelMax;
		this.requestedFrameMax = requestedFrameMax;
		this.requestedHeartbeat = requestedHeartbeat;
	}

	/**
	*
	* @param uri the connection URI
	* @param networkRecoveryInterval connection recovery interval in milliseconds
	* @param automaticRecovery if automatic connection recovery
	* @param topologyRecovery if topology recovery
	* @param connectionTimeout connection timeout
	* @param requestedChannelMax requested maximum channel number
	* @param requestedFrameMax requested maximum frame size
	* @param requestedHeartbeat requested heartbeat interval
	* @throws NullPointerException if URI is null
	*/
	private RMQConnectionConfig(String uri, Integer networkRecoveryInterval, Boolean automaticRecovery,
								Boolean topologyRecovery, Integer connectionTimeout, Integer requestedChannelMax,
								Integer requestedFrameMax, Integer requestedHeartbeat){
		Preconditions.checkNotNull(uri, "Uri can not be null");
		this.uri = uri;

		this.networkRecoveryInterval = networkRecoveryInterval;
		this.automaticRecovery = automaticRecovery;
		this.topologyRecovery = topologyRecovery;
		this.connectionTimeout = connectionTimeout;
		this.requestedChannelMax = requestedChannelMax;
		this.requestedFrameMax = requestedFrameMax;
		this.requestedHeartbeat = requestedHeartbeat;
	}

	/** @return the host to use for connections */
	public String getHost() {
		return host;
	}

	/** @return the port to use for connections */
	public int getPort() {
		return port;
	}

	/**
	 * Retrieve the virtual host.
	 * @return the virtual host to use when connecting to the broker
	 */
	public String getVirtualHost() {
		return virtualHost;
	}

	/**
	 * Retrieve the user name.
	 * @return the AMQP user name to use when connecting to the broker
	 */
	public String getUsername() {
		return username;
	}

	/**
	 * Retrieve the password.
	 * @return the password to use when connecting to the broker
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * Retrieve the URI.
	 * @return the connection URI when connecting to the broker
     */
	public String getUri() {
		return uri;
	}

	/**
	 * Returns automatic connection recovery interval in milliseconds.
	 * @return how long will automatic recovery wait before attempting to reconnect, in ms; default is 5000
	 */
	public Integer getNetworkRecoveryInterval() {
		return networkRecoveryInterval;
	}

	/**
	 * Returns true if automatic connection recovery is enabled, false otherwise
	 * @return true if automatic connection recovery is enabled, false otherwise
	 */
	public Boolean isAutomaticRecovery() {
		return automaticRecovery;
	}

	/**
	 * Returns true if topology recovery is enabled, false otherwise
	 * @return true if topology recovery is enabled, false otherwise
	 */
	public Boolean isTopologyRecovery() {
		return topologyRecovery;
	}

	/**
	 * Retrieve the connection timeout.
	 * @return the connection timeout, in milliseconds; zero for infinite
	 */
	public Integer getConnectionTimeout() {
		return connectionTimeout;
	}

	/**
	 * Retrieve the requested maximum channel number
	 * @return the initially requested maximum channel number; zero for unlimited
	 */
	public Integer getRequestedChannelMax() {
		return requestedChannelMax;
	}

	/**
	 * Retrieve the requested maximum frame size
	 * @return the initially requested maximum frame size, in octets; zero for unlimited
	 */
	public Integer getRequestedFrameMax() {
		return requestedFrameMax;
	}

	/**
	 * Retrieve the requested heartbeat interval.
	 * @return the initially requested heartbeat interval, in seconds; zero for none
	 */
	public Integer getRequestedHeartbeat() {
		return requestedHeartbeat;
	}

	/**
	 *
	 * @return Connection Factory for RMQ
	 * @throws URISyntaxException, NoSuchAlgorithmException, KeyManagementException if Malformed URI has been passed
     */
	public ConnectionFactory getConnectionFactory() throws URISyntaxException,
		NoSuchAlgorithmException, KeyManagementException {
		ConnectionFactory factory = new ConnectionFactory();
		if (this.uri != null && !this.uri.isEmpty()){
			try {
				factory.setUri(getUri());
			}catch (URISyntaxException | NoSuchAlgorithmException | KeyManagementException e){
				LOG.error("Failed to parse uri {}", e.getMessage());
				throw e;
			}
		} else {
			factory.setHost(this.host);
			factory.setPort(this.port);
			factory.setVirtualHost(this.virtualHost);
			factory.setUsername(this.username);
			factory.setPassword(this.password);
		}

		if (this.automaticRecovery != null) {
			factory.setAutomaticRecoveryEnabled(this.automaticRecovery);
		}
		if (this.connectionTimeout != null) {
			factory.setConnectionTimeout(this.connectionTimeout);
		}
		if (this.networkRecoveryInterval != null) {
			factory.setNetworkRecoveryInterval(this.networkRecoveryInterval);
		}
		if (this.requestedHeartbeat != null) {
			factory.setRequestedHeartbeat(this.requestedHeartbeat);
		}
		if (this.topologyRecovery != null) {
			factory.setTopologyRecoveryEnabled(this.topologyRecovery);
		}
		if (this.requestedChannelMax != null) {
			factory.setRequestedChannelMax(this.requestedChannelMax);
		}
		if (this.requestedFrameMax != null) {
			factory.setRequestedFrameMax(this.requestedFrameMax);
		}

		return factory;
	}

	/**
	 * The Builder Class for {@link RMQConnectionConfig}
	 */
	public static class Builder {

		private String host;
		private Integer port;
		private String virtualHost;
		private String username;
		private String password;

		private Integer networkRecoveryInterval;
		private Boolean automaticRecovery;
		private Boolean topologyRecovery;

		private Integer connectionTimeout;
		private Integer requestedChannelMax;
		private Integer requestedFrameMax;
		private Integer requestedHeartbeat;

		private String uri;

		/**
		 * Set the target port.
		 * @param port the default port to use for connections
		 * @return the Builder
		 */
		public Builder setPort(int port) {
			this.port = port;
			return this;
		}

		/** @param host the default host to use for connections
		 * @return the Builder
		 */
		public Builder setHost(String host) {
			this.host = host;
			return this;
		}

		/**
		 * Set the virtual host.
		 * @param virtualHost the virtual host to use when connecting to the broker
		 * @return the Builder
		 */
		public Builder setVirtualHost(String virtualHost) {
			this.virtualHost = virtualHost;
			return this;
		}

		/**
		 * Set the user name.
		 * @param username the AMQP user name to use when connecting to the broker
		 * @return the Builder
		 */
		public Builder setUserName(String username) {
			this.username = username;
			return this;
		}

		/**
		 * Set the password.
		 * @param password the password to use when connecting to the broker
		 * @return the Builder
		 */
		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		/**
		 * Convenience method for setting the fields in an AMQP URI: host,
		 * port, username, password and virtual host.  If any part of the
		 * URI is ommited, the ConnectionFactory's corresponding variable
		 * is left unchanged.
		 * @param uri is the AMQP URI containing the data
		 * @return the Builder
		 */
		public Builder setUri(String uri) {
			this.uri = uri;
			return this;
		}

		/**
		 * Enables or disables topology recovery
		 * @param topologyRecovery if true, enables topology recovery
		 * @return the Builder
		 */
		public Builder setTopologyRecoveryEnabled(boolean topologyRecovery) {
			this.topologyRecovery = topologyRecovery;
			return this;
		}

		/**
		 * Set the requested heartbeat.
		 * @param requestedHeartbeat the initially requested heartbeat interval, in seconds; zero for none
		 * @return the Builder
		 */
		public Builder setRequestedHeartbeat(int requestedHeartbeat) {
			this.requestedHeartbeat = requestedHeartbeat;
			return this;
		}

		/**
		 * Set the requested maximum frame size
		 * @param requestedFrameMax initially requested maximum frame size, in octets; zero for unlimited
		 * @return the Builder
		 */
		public Builder setRequestedFrameMax(int requestedFrameMax) {
			this.requestedFrameMax = requestedFrameMax;
			return this;
		}

		/**
		 * Set the requested maximum channel number
		 * @param requestedChannelMax initially requested maximum channel number; zero for unlimited
		 */
		public Builder setRequestedChannelMax(int requestedChannelMax) {
			this.requestedChannelMax = requestedChannelMax;
			return this;
		}

		/**
		 * Sets connection recovery interval. Default is 5000.
		 * @param networkRecoveryInterval how long will automatic recovery wait before attempting to reconnect, in ms
		 * @return the Builder
		 */
		public Builder setNetworkRecoveryInterval(int networkRecoveryInterval) {
			this.networkRecoveryInterval = networkRecoveryInterval;
			return this;
		}

		/**
		 * Set the connection timeout.
		 * @param connectionTimeout connection establishment timeout in milliseconds; zero for infinite
		 * @return the Builder
		 */
		public Builder setConnectionTimeout(int connectionTimeout) {
			this.connectionTimeout = connectionTimeout;
			return this;
		}

		/**
		 * Enables or disables automatic connection recovery
		 * @param automaticRecovery if true, enables connection recovery
		 * @return the Builder
		 */
		public Builder setAutomaticRecovery(boolean automaticRecovery) {
			this.automaticRecovery = automaticRecovery;
			return this;
		}

		/**
		 * The Builder method
		 * If URI is NULL we use host, port, vHost, username, password combination
		 * to initialize connection. using  {@link RMQConnectionConfig#RMQConnectionConfig(String, Integer, String, String, String,
		 * Integer, Boolean, Boolean, Integer, Integer, Integer, Integer)}
		 *
		 * else URI will be used to initialize the client connection
		 * {@link RMQConnectionConfig#RMQConnectionConfig(String, Integer, Boolean, Boolean, Integer, Integer, Integer, Integer)}
		 * @return RMQConnectionConfig
         */
		public RMQConnectionConfig build(){
			if(this.uri != null) {
				return new RMQConnectionConfig(this.uri, this.networkRecoveryInterval,
					this.automaticRecovery, this.topologyRecovery, this.connectionTimeout, this.requestedChannelMax,
					this.requestedFrameMax, this.requestedHeartbeat);
			} else {
				return new RMQConnectionConfig(this.host, this.port, this.virtualHost, this.username, this.password,
					this.networkRecoveryInterval, this.automaticRecovery, this.topologyRecovery,
					this.connectionTimeout, this.requestedChannelMax, this.requestedFrameMax, this.requestedHeartbeat);
			}
		}
	}
}
