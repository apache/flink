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

package org.apache.flink.connector.rabbitmq2.sink.common;

import com.rabbitmq.client.ReturnListener;

import java.io.Serializable;

/**
 * This class was copied from the old RabbitMQ connector. A serializable {@link ReturnListener} to
 * handle unroutable but "mandatory" messages.
 *
 * <p>If a message has the "mandatory" flag set, but cannot be routed, RabbitMQ's broker will return
 * the message to the publishing client (via an AMQP.Basic.Return command). This ReturnListener
 * implements a callback handler to get notified in such returns and act on these messages as
 * wanted.
 */
public interface SerializableReturnListener extends Serializable, ReturnListener {}
