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

package org.apache.flink.table.secret;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Base marker interface for secret store implementations.
 *
 * <p>This interface serves as the common base for both {@link ReadableSecretStore} and {@link
 * WritableSecretStore}, allowing for flexible secret management implementations.
 *
 * <p>Secret stores are used to manage sensitive configuration data (credentials, tokens, passwords,
 * etc.) in Flink SQL and Table API applications.
 */
@PublicEvolving
public interface SecretStore {}
