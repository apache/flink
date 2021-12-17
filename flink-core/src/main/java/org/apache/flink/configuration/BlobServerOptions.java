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

package org.apache.flink.configuration;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.TextElement.code;

/** Configuration options for the BlobServer and BlobCache. */
@PublicEvolving
public class BlobServerOptions {

    /** The config parameter defining the storage directory to be used by the blob server. */
    public static final ConfigOption<String> STORAGE_DIRECTORY =
            key("blob.storage.directory")
                    .noDefaultValue()
                    .withDescription(
                            "The config parameter defining the storage directory to be used by the blob server.");

    /** The config parameter defining number of retires for failed BLOB fetches. */
    public static final ConfigOption<Integer> FETCH_RETRIES =
            key("blob.fetch.retries")
                    .defaultValue(5)
                    .withDescription(
                            "The config parameter defining number of retires for failed BLOB fetches.");

    /**
     * The config parameter defining the maximum number of concurrent BLOB fetches that the
     * JobManager serves.
     */
    public static final ConfigOption<Integer> FETCH_CONCURRENT =
            key("blob.fetch.num-concurrent")
                    .defaultValue(50)
                    .withDescription(
                            "The config parameter defining the maximum number of concurrent BLOB fetches that the JobManager serves.");

    /** The config parameter defining the backlog of BLOB fetches on the JobManager. */
    public static final ConfigOption<Integer> FETCH_BACKLOG =
            key("blob.fetch.backlog")
                    .defaultValue(1000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The config parameter defining the desired backlog of BLOB fetches on the JobManager."
                                                    + "Note that the operating system usually enforces an upper limit on the backlog size based on the %s setting.",
                                            code("SOMAXCONN"))
                                    .build());

    /**
     * The config parameter defining the server port of the blob service. The port can either be a
     * port, such as "9123", a range of ports: "50100-50200" or a list of ranges and or points:
     * "50100-50200,50300-50400,51234"
     *
     * <p>Setting the port to 0 will let the OS choose an available port.
     */
    public static final ConfigOption<String> PORT =
            key("blob.server.port")
                    .defaultValue("0")
                    .withDescription(
                            "The config parameter defining the server port of the blob service.");

    /** Flag to override ssl support for the blob service transport. */
    public static final ConfigOption<Boolean> SSL_ENABLED =
            key("blob.service.ssl.enabled")
                    .defaultValue(true)
                    .withDescription(
                            "Flag to override ssl support for the blob service transport.");

    /**
     * Cleanup interval of the blob caches at the task managers (in seconds).
     *
     * <p>Whenever a job is not referenced at the cache anymore, we set a TTL and let the periodic
     * cleanup task (executed every CLEANUP_INTERVAL seconds) remove its blob files after this TTL
     * has passed. This means that a blob will be retained at most <tt>2 * CLEANUP_INTERVAL</tt>
     * seconds after not being referenced anymore. Therefore, a recovery still has the chance to use
     * existing files rather than to download them again.
     */
    public static final ConfigOption<Long> CLEANUP_INTERVAL =
            key("blob.service.cleanup.interval")
                    .defaultValue(3_600L) // once per hour
                    .withDeprecatedKeys("library-cache-manager.cleanup.interval")
                    .withDescription(
                            "Cleanup interval of the blob caches at the task managers (in seconds).");

    /** The minimum size for messages to be offloaded to the BlobServer. */
    public static final ConfigOption<Integer> OFFLOAD_MINSIZE =
            key("blob.offload.minsize")
                    .defaultValue(1_024 * 1_024) // 1MiB by default
                    .withDescription(
                            "The minimum size for messages to be offloaded to the BlobServer.");

    /** The socket timeout in milliseconds for the blob client. */
    public static final ConfigOption<Integer> SO_TIMEOUT =
            key("blob.client.socket.timeout")
                    .defaultValue(300_000)
                    .withDescription("The socket timeout in milliseconds for the blob client.");

    /** The connection timeout in milliseconds for the blob client. */
    public static final ConfigOption<Integer> CONNECT_TIMEOUT =
            key("blob.client.connect.timeout")
                    .defaultValue(0)
                    .withDescription("The connection timeout in milliseconds for the blob client.");
}
