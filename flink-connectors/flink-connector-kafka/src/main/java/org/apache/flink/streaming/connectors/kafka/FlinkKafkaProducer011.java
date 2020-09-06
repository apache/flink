/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;

/**
 * Compatibility class to make migration possible from the 0.11 connector to the universal one.
 *
 * <p>Problem is that FlinkKafkaProducer (universal) and FlinkKafkaProducer011 have different names and
 * they both defined static classes NextTransactionalIdHint, KafkaTransactionState and
 * KafkaTransactionContext inside the parent classes. This is causing incompatibility problems since
 * for example FlinkKafkaProducer011.KafkaTransactionState and FlinkKafkaProducer.KafkaTransactionState
 * are treated as completely incompatible classes, despite being identical.
 *
 * <p>This issue is solved by using custom serialization logic: keeping a fake/dummy
 * FlinkKafkaProducer011.*Serializer classes in the universal connector
 * (this class), as entry points for the deserialization and converting them to
 * FlinkKafkaProducer.*Serializer counter parts. After all serialized binary data are exactly
 * the same in all of those cases.
 *
 * <p>For more details check FLINK-11249 and the discussion in the pull requests.
 */
//CHECKSTYLE:OFF: JavadocType
public class FlinkKafkaProducer011 {
	public static class NextTransactionalIdHintSerializer {
		public static final class NextTransactionalIdHintSerializerSnapshot extends SimpleTypeSerializerSnapshot<FlinkKafkaProducer.NextTransactionalIdHint> {
			public NextTransactionalIdHintSerializerSnapshot() {
				super(FlinkKafkaProducer.NextTransactionalIdHintSerializer::new);
			}
		}
	}

	public static class ContextStateSerializer {
		public static final class ContextStateSerializerSnapshot extends SimpleTypeSerializerSnapshot<FlinkKafkaProducer.KafkaTransactionContext> {
			public ContextStateSerializerSnapshot() {
				super(FlinkKafkaProducer.ContextStateSerializer::new);
			}
		}
	}

	public static class TransactionStateSerializer {
		public static final class TransactionStateSerializerSnapshot extends SimpleTypeSerializerSnapshot<FlinkKafkaProducer.KafkaTransactionState> {
			public TransactionStateSerializerSnapshot() {
				super(FlinkKafkaProducer.TransactionStateSerializer::new);
			}
		}
	}

	public static class NextTransactionalIdHint extends FlinkKafkaProducer.NextTransactionalIdHint {
	}
}
//CHECKSTYLE:ON: JavadocType
