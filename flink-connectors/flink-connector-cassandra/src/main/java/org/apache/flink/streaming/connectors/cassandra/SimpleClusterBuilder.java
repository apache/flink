/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metrics;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.policies.AddressTranslator;
import com.datastax.driver.core.policies.ConstantReconnectionPolicy;
import com.datastax.driver.core.policies.ConstantSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.ExponentialReconnectionPolicy;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.NoSpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.Policies;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;
import com.datastax.driver.core.policies.RoundRobinPolicy;
import com.datastax.driver.core.policies.SpeculativeExecutionPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import java.io.Serializable;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

/** A Simple ClusterBuilder which is currently used in PyFlink Cassandra connector. */
public class SimpleClusterBuilder extends ClusterBuilder implements Serializable {

    private static final long serialVersionUID = 1L;

    // Transit map for builder cluster
    public final Map<String, Object> clusterMap;
    // ClusterBuilder method constant
    private static final String CLUSTER_NAME = "clusterName";
    private static final String CLUSTER_PORT = "clusterPort";
    private static final String ALLOW_BETA_PROTOCOL_VERSION = "allowBetaProtocolVersion";
    private static final String MAX_SCHEMA_AGREEMENT_WAIT_SECONDS = "maxSchemaAgreementWaitSeconds";
    private static final String CONTACT_POINT = "contactPoint";
    private static final String CONTACT_POINTS = "contactPoints";
    private static final String CONTACT_POINTS_WITH_PORTS = "contactPointsWithPorts";
    private static final String LOAD_BALANCING_POLICY = "loadBalancingPolicy";
    private static final String RECONNECTION_POLICY = "reconnectionPolicy";
    private static final String RETRY_POLICY = "retryPolicy";
    private static final String SPECULATIVE_EXECUTION_POLICY = "speculativeExecutionPolicy";
    private static final String CREDENTIALS = "credentials";
    private static final String WITHOUT_METRICS = "withoutMetrics";
    private static final String WITHOUT_JMX_REPORTING = "withoutJMXReporting";
    private static final String NO_COMPACT = "noCompact";
    // LoadBalancingPolicies support for PyFlink
    private static final String ROUND_ROBIN_POLICY = "roundRobinPolicy";
    private static final String DC_AWARE_ROUND_ROBIN_POLICY = "dCAwareRoundRobinPolicy";
    // ReconnectionPolicies support for PyFlink
    private static final String EXPONENTIAL_RECONNECTION_POLICY = "exponentialReconnectionPolicy";
    private static final String CONSTANT_RECONNECTION_POLICY = "constantReconnectionPolicy";
    // RetryPolicies support for PyFlink
    private static final String CONSISTENCY_RETRY_POLICY = "consistencyRetryPolicy";
    private static final String FALLTHROUGH_RETRY_POLICY = "fallthroughRetryPolicy";
    // SpeculativeExecutionPolicies support for PyFlink
    private static final String NO_SPECULATIVE_EXECUTION_POLICY = "noSpeculativeExecutionPolicy";
    private static final String CONSTANT_SPECULATIVE_EXECUTION_POLICY =
            "constantSpeculativeExecutionPolicy";
    // True index for mark the configuration take effect
    private static final String TRUE_INDEX = "true";

    public SimpleClusterBuilder() {
        clusterMap = new HashMap<>();
    }

    /**
     * An optional name for the create cluster.
     *
     * <p>Note: this is not related to the Cassandra cluster name (though you are free to provide
     * the same name). See {@link Cluster#getClusterName} for details.
     *
     * <p>If you use this method and create more than one Cluster instance in the same JVM (which
     * should be avoided unless you need to connect to multiple Cassandra clusters), you should make
     * sure each Cluster instance get a unique name or you may have a problem with JMX reporting.
     *
     * @param name the cluster name to use for the created Cluster instance.
     * @return this Builder.
     */
    public SimpleClusterBuilder withClusterName(String name) {
        this.clusterMap.put(CLUSTER_NAME, name);
        return this;
    }

    /**
     * The port to use to connect to the Cassandra host.
     *
     * <p>If not set through this method, the default port (9042) will be used instead.
     *
     * @param port the port to set.
     * @return this Builder.
     */
    public SimpleClusterBuilder withPort(int port) {
        this.clusterMap.put(CLUSTER_PORT, port);
        return this;
    }

    /**
     * Create cluster connection using latest development protocol version, which is currently in
     * beta. Calling this method will result into setting USE_BETA flag in all outgoing messages,
     * which allows server to negotiate the supported protocol version even if it is currently in
     * beta.
     *
     * <p>This feature is only available starting with version {@link ProtocolVersion#V5 V5}.
     *
     * <p>Use with caution, refer to the server and protocol documentation for the details on latest
     * protocol version.
     *
     * @return this Builder.
     */
    public SimpleClusterBuilder allowBetaProtocolVersion() {
        this.clusterMap.put(ALLOW_BETA_PROTOCOL_VERSION, TRUE_INDEX);
        return this;
    }

    /**
     * Sets the maximum time to wait for schema agreement before returning from a DDL query.
     *
     * <p>If not set through this method, the default value (10 seconds) will be used.
     *
     * @param maxSchemaAgreementWaitSeconds the new value to set.
     * @return this Builder.
     * @throws IllegalStateException if the provided value is zero or less.
     */
    public SimpleClusterBuilder withMaxSchemaAgreementWaitSeconds(
            int maxSchemaAgreementWaitSeconds) {
        this.clusterMap.put(MAX_SCHEMA_AGREEMENT_WAIT_SECONDS, maxSchemaAgreementWaitSeconds);
        return this;
    }

    /**
     * Adds a contact point - or many if the given address resolves to multiple <code>InetAddress
     * </code>s (A records).
     *
     * <p>Contact points are addresses of Cassandra nodes that the driver uses to discover the
     * cluster topology. Only one contact point is required (the driver will retrieve the address of
     * the other nodes automatically), but it is usually a good idea to provide more than one
     * contact point, because if that single contact point is unavailable, the driver cannot
     * initialize itself correctly.
     *
     * <p>Note that by default (that is, unless you use the {@link #withLoadBalancingPolicy}) method
     * of this builder), the first successfully contacted host will be used to define the local
     * data-center for the client. If follows that if you are running Cassandra in a multiple
     * data-center setting, it is a good idea to only provide contact points that are in the same
     * datacenter than the client, or to provide manually the load balancing policy that suits your
     * need.
     *
     * <p>If the host name points to a DNS record with multiple a-records, all InetAddresses
     * returned will be used. Make sure that all resulting <code>InetAddress</code>s returned point
     * to the same cluster and datacenter.
     *
     * @param address the address of the node(s) to connect to.
     * @return this Builder.
     * @throws IllegalArgumentException if the given {@code address} could not be resolved.
     * @throws SecurityException if a security manager is present and permission to resolve the host
     *     name is denied.
     */
    public SimpleClusterBuilder addContactPoint(String address) {
        this.clusterMap.put(CONTACT_POINT, address);
        return this;
    }

    /**
     * Adds contact points.
     *
     * <p>See {@link Cluster.Builder#addContactPoint} for more details on contact points.
     *
     * <p>Note that all contact points must be resolvable; if <em>any</em> of them cannot be
     * resolved, this method will fail.
     *
     * @param addresses addresses of the nodes to add as contact points.
     * @return this Builder.
     * @throws IllegalArgumentException if any of the given {@code addresses} could not be resolved.
     * @throws SecurityException if a security manager is present and permission to resolve the host
     *     name is denied.
     * @see Cluster.Builder#addContactPoint
     */
    public SimpleClusterBuilder addContactPoints(String... addresses) {
        this.clusterMap.put(CONTACT_POINTS, addresses);
        return this;
    }

    /**
     * Adds contact points.
     *
     * <p>See {@link Cluster.Builder#addContactPoint} for more details on contact points. Contrarily
     * to other {@code addContactPoints} methods, this method allows to provide a different port for
     * each contact point. Since Cassandra nodes must always all listen on the same port, this is
     * rarely what you want and most users should prefer other {@code addContactPoints} methods to
     * this one. However, this can be useful if the Cassandra nodes are behind a router and are not
     * accessed directly. Note that if you are in this situation (Cassandra nodes are behind a
     * router, not directly accessible), you almost surely want to provide a specific {@link
     * AddressTranslator} (through {@link #withAddressTranslator}) to translate actual Cassandra
     * node addresses to the addresses the driver should use, otherwise the driver will not be able
     * to auto-detect new nodes (and will generally not function optimally).
     *
     * @param addresses addresses of the nodes to add as contact points.
     * @return this Builder
     * @see Cluster.Builder#addContactPoint
     */
    public SimpleClusterBuilder addContactPointsWithPorts(InetSocketAddress... addresses) {
        this.clusterMap.put(CONTACT_POINTS_WITH_PORTS, addresses);
        return this;
    }

    /**
     * Configures the load balancing policy to use for the new cluster.
     *
     * <p>If no load balancing policy is set through this method, {@link
     * Policies#defaultLoadBalancingPolicy} will be used instead.
     *
     * @param policy the load balancing policy to use.
     * @return this Builder.
     */
    public SimpleClusterBuilder withLoadBalancingPolicy(LoadBalancingPolicy policy) {
        if (policy instanceof RoundRobinPolicy) {
            this.clusterMap.put(LOAD_BALANCING_POLICY, ROUND_ROBIN_POLICY);
        } else if (policy instanceof DCAwareRoundRobinPolicy) {
            this.clusterMap.put(LOAD_BALANCING_POLICY, DC_AWARE_ROUND_ROBIN_POLICY);
        }
        return this;
    }

    /**
     * Configures the reconnection policy to use for the new cluster.
     *
     * <p>If no reconnection policy is set through this method, {@link
     * Policies#DEFAULT_RECONNECTION_POLICY} will be used instead.
     *
     * @param policy the reconnection policy to use.
     * @return this Builder.
     */
    public SimpleClusterBuilder withReconnectionPolicy(ReconnectionPolicy policy) {
        StringBuilder builder = new StringBuilder();

        if (policy instanceof ExponentialReconnectionPolicy) {
            long baseDelayMs = ((ExponentialReconnectionPolicy) policy).getBaseDelayMs();
            long maxDelayMs = ((ExponentialReconnectionPolicy) policy).getMaxDelayMs();
            String policyInfo =
                    builder.append(EXPONENTIAL_RECONNECTION_POLICY)
                            .append("|")
                            .append(baseDelayMs)
                            .append("|")
                            .append(maxDelayMs)
                            .toString();
            this.clusterMap.put(RECONNECTION_POLICY, policyInfo);
        } else if (policy instanceof ConstantReconnectionPolicy) {
            long constantDelayMs = ((ConstantReconnectionPolicy) policy).getConstantDelayMs();
            String policyInfo =
                    builder.append(CONSTANT_RECONNECTION_POLICY)
                            .append("|")
                            .append(constantDelayMs)
                            .toString();
            this.clusterMap.put(RECONNECTION_POLICY, policyInfo);
        }
        return this;
    }

    /**
     * Configures the retry policy to use for the new cluster.
     *
     * <p>If no retry policy is set through this method, {@link Policies#DEFAULT_RETRY_POLICY} will
     * be used instead.
     *
     * @param policy the retry policy to use.
     * @return this Builder.
     */
    public SimpleClusterBuilder withRetryPolicy(RetryPolicy policy) {
        if (policy instanceof DefaultRetryPolicy) {
            this.clusterMap.put(RETRY_POLICY, CONSISTENCY_RETRY_POLICY);
        } else if (policy instanceof FallthroughRetryPolicy) {
            this.clusterMap.put(RETRY_POLICY, FALLTHROUGH_RETRY_POLICY);
        }
        return this;
    }

    /**
     * Configures the speculative execution policy to use for the new cluster.
     *
     * <p>If no policy is set through this method, {@link
     * Policies#defaultSpeculativeExecutionPolicy()} will be used instead.
     *
     * @param policy the policy to use.
     * @return this Builder.
     */
    public SimpleClusterBuilder withSpeculativeExecutionPolicy(SpeculativeExecutionPolicy policy) {
        if (policy instanceof NoSpeculativeExecutionPolicy) {
            this.clusterMap.put(SPECULATIVE_EXECUTION_POLICY, NO_SPECULATIVE_EXECUTION_POLICY);
        } else if (policy instanceof ConstantSpeculativeExecutionPolicyExt) {
            long delayMillis = ((ConstantSpeculativeExecutionPolicyExt) policy).getDelayMillis();
            int maxExecutions = ((ConstantSpeculativeExecutionPolicyExt) policy).getMaxExecutions();
            String policyInfo =
                    CONSTANT_SPECULATIVE_EXECUTION_POLICY + "|" + delayMillis + "|" + maxExecutions;
            this.clusterMap.put(SPECULATIVE_EXECUTION_POLICY, policyInfo);
        }
        return this;
    }

    /**
     * Uses the provided credentials when connecting to Cassandra hosts.
     *
     * <p>This should be used if the Cassandra cluster has been configured to use the {@code
     * PasswordAuthenticator}. If the the default {@code AllowAllAuthenticator} is used instead,
     * using this method has no effect.
     *
     * @param username the username to use to login to Cassandra hosts.
     * @param password the password corresponding to {@code username}.
     * @return this Builder.
     */
    public SimpleClusterBuilder withCredentials(String username, String password) {
        String credentialsInfo = CREDENTIALS + "|" + username + "|" + password;
        this.clusterMap.put(CREDENTIALS, credentialsInfo);
        return this;
    }

    /**
     * Disables metrics collection for the created cluster (metrics are enabled by default
     * otherwise).
     *
     * @return this builder.
     */
    public SimpleClusterBuilder withoutMetrics() {
        this.clusterMap.put(WITHOUT_METRICS, TRUE_INDEX);
        return this;
    }

    /**
     * Disables JMX reporting of the metrics.
     *
     * <p>JMX reporting is enabled by default (see {@link Metrics}) but can be disabled using this
     * option. If metrics are disabled, this is a no-op.
     *
     * @return this builder.
     */
    public SimpleClusterBuilder withoutJMXReporting() {
        this.clusterMap.put(WITHOUT_JMX_REPORTING, TRUE_INDEX);
        return this;
    }

    /**
     * Enables the <code>NO_COMPACT</code> startup option.
     *
     * @return this builder.
     * @see <a href="https://issues.apache.org/jira/browse/CASSANDRA-10857">CASSANDRA-10857</a>
     */
    public SimpleClusterBuilder withNoCompact() {
        this.clusterMap.put(NO_COMPACT, TRUE_INDEX);
        return this;
    }

    /**
     * Configures the connection to Cassandra. The configuration is done by calling methods on the
     * builder object and finalizing the configuration with build().
     *
     * @param builder connection builder
     * @return configured connection
     */
    @Override
    protected Cluster buildCluster(Cluster.Builder builder) {
        String clusterName = (String) this.clusterMap.get(CLUSTER_NAME);
        String contactPoint = (String) this.clusterMap.get(CONTACT_POINT);
        String[] contactPoints = (String[]) this.clusterMap.get(CONTACT_POINTS);
        String loadBalancingPolicy = (String) this.clusterMap.get(LOAD_BALANCING_POLICY);
        String reconnectionPolicy = (String) this.clusterMap.get(RECONNECTION_POLICY);
        String retryPolicy = (String) this.clusterMap.get(RETRY_POLICY);
        String credentials = (String) this.clusterMap.get(CREDENTIALS);
        String metrics = (String) this.clusterMap.get(WITHOUT_METRICS);
        String noCompact = (String) this.clusterMap.get(NO_COMPACT);
        String jMXReporting = (String) this.clusterMap.get(WITHOUT_JMX_REPORTING);
        String allowBetaProtocolVersion = (String) this.clusterMap.get(ALLOW_BETA_PROTOCOL_VERSION);
        Integer port = (Integer) this.clusterMap.get(CLUSTER_PORT);
        String speculativeExecutionPolicy =
                (String) this.clusterMap.get(SPECULATIVE_EXECUTION_POLICY);
        InetSocketAddress[] contactPointsWithPorts =
                (InetSocketAddress[]) this.clusterMap.get(CONTACT_POINTS_WITH_PORTS);
        Integer maxSchemaAgreementWaitSeconds =
                (Integer) this.clusterMap.get(MAX_SCHEMA_AGREEMENT_WAIT_SECONDS);
        if (clusterName != null) {
            builder.withClusterName(clusterName);
        }
        if (port != null) {
            builder.withPort(port);
        }
        if (allowBetaProtocolVersion != null) {
            builder.allowBetaProtocolVersion();
        }
        if (maxSchemaAgreementWaitSeconds != null) {
            builder.withMaxSchemaAgreementWaitSeconds(maxSchemaAgreementWaitSeconds);
        }
        if (contactPoint != null) {
            builder.addContactPoint(contactPoint);
        }
        if (contactPoints != null) {
            builder.addContactPoints(contactPoints);
        }
        if (contactPointsWithPorts != null) {
            builder.addContactPointsWithPorts(contactPointsWithPorts);
        }
        if (loadBalancingPolicy != null) {
            if (ROUND_ROBIN_POLICY.equals(loadBalancingPolicy)) {
                builder.withLoadBalancingPolicy(new RoundRobinPolicy());
            } else if (DC_AWARE_ROUND_ROBIN_POLICY.equals(loadBalancingPolicy)) {
                builder.withLoadBalancingPolicy(
                        new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()));
            }
        }
        if (reconnectionPolicy != null) {
            String[] split = reconnectionPolicy.split("\\|");
            String name = split[0];

            if (EXPONENTIAL_RECONNECTION_POLICY.equals(name)) {
                int baseDelayMs = Integer.parseInt(split[1]);
                int maxDelayMs = Integer.parseInt(split[2]);
                builder.withReconnectionPolicy(
                        new ExponentialReconnectionPolicy(baseDelayMs, maxDelayMs));
            } else if (CONSTANT_RECONNECTION_POLICY.equals(name)) {
                int constantDelayMs = Integer.parseInt(split[1]);
                builder.withReconnectionPolicy(new ConstantReconnectionPolicy(constantDelayMs));
            }
        }
        if (retryPolicy != null) {
            if (CONSISTENCY_RETRY_POLICY.equals(retryPolicy)) {
                builder.withRetryPolicy(Policies.defaultRetryPolicy());
            } else if (FALLTHROUGH_RETRY_POLICY.equals(retryPolicy)) {
                builder.withRetryPolicy(FallthroughRetryPolicy.INSTANCE);
            }
        }
        if (speculativeExecutionPolicy != null) {
            if (NO_SPECULATIVE_EXECUTION_POLICY.equals(speculativeExecutionPolicy)) {
                builder.withSpeculativeExecutionPolicy(NoSpeculativeExecutionPolicy.INSTANCE);
            }
        }
        if (credentials != null) {
            String[] split = credentials.split("\\|");
            String userName = split[1];
            String passWord = split[2];
            builder.withCredentials(userName, passWord);
        }
        if (jMXReporting != null) {
            builder.withoutJMXReporting();
        }
        if (metrics != null) {
            builder.withoutMetrics();
        }
        if (noCompact != null) {
            builder.withNoCompact();
        }
        return builder.build();
    }
}

/** Extend ConstantSpeculativeExecutionPolicy. */
class ConstantSpeculativeExecutionPolicyExt extends ConstantSpeculativeExecutionPolicy {
    /** the number of speculative executions. Must be strictly positive. */
    private final int maxExecutions;
    /**
     * The delay between each speculative execution. Must be >= 0. A zero delay means it should
     * immediately send `maxSpeculativeExecutions` requests along with the original request.
     */
    private final long delayMillis;
    /**
     * Builds a new instance.
     *
     * @param delayMillis the delay between each speculative execution. Must be >= 0. A zero delay
     *     means it should immediately send `maxSpeculativeExecutions` requests along with the
     *     original request.
     * @param maxExecutions the number of speculative executions. Must be strictly positive.
     * @throws IllegalArgumentException if one of the arguments does not respect the preconditions
     *     above.
     */
    public ConstantSpeculativeExecutionPolicyExt(long delayMillis, int maxExecutions) {
        super(delayMillis, maxExecutions);
        this.delayMillis = delayMillis;
        this.maxExecutions = maxExecutions;
    }

    /** Get the number of speculative executions. */
    public int getMaxExecutions() {
        return maxExecutions;
    }

    /** Get the delay between each speculative execution. */
    public long getDelayMillis() {
        return delayMillis;
    }
}
