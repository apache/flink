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

package org.apache.flink.streaming.connectors.pulsar.internal;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PulsarMetadataReader implements Closeable {

	private PulsarAdmin admin;

	private volatile boolean closed = false;

	public PulsarMetadataReader(String adminUrl) throws PulsarClientException {
		this.admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build();
	}

	@Override
	public void close() {
		closed = true;
		if (admin != null) {
			admin.close();
			admin = null;
		}
	}
	
	public List<String> listNamespaces() throws PulsarAdminException {
		List<String> tenants = admin.tenants().getTenants();
		List<String> namespaces = new ArrayList<>();
		for (String tenant : tenants) {
			namespaces.addAll(admin.namespaces().getNamespaces(tenant));
		}
		return namespaces;
	}

	public boolean namespaceExists(String ns) throws PulsarAdminException {
		try {
			admin.namespaces().getTopics(ns);
		} catch (PulsarAdminException.NotFoundException e) {
			return false;
		}
		return true;
	}

	public void createNamespace(String ns) throws PulsarAdminException {
		String nsName = NamespaceName.get(ns).toString();
		admin.namespaces().createNamespace(nsName);
	}

	public void deleteNamespace(String ns) throws PulsarAdminException {
		String nsName = NamespaceName.get(ns).toString();
		admin.namespaces().deleteNamespace(nsName);
	}

	public List<String> getTopics(String ns) throws PulsarAdminException {
		List<String> nonPartitionedTopics = getNonPartitionedTopics(ns);
		List<String> partitionedTopics = admin.topics().getPartitionedTopicList(ns);
		List<String> allTopics = new ArrayList<>();
		Stream.of(partitionedTopics, nonPartitionedTopics).forEach(allTopics::addAll);
		return allTopics.stream().map(t -> TopicName.get(t).getLocalName()).collect(Collectors.toList());
	}

	private List<String> getNonPartitionedTopics(String databaseName) throws PulsarAdminException {
		return admin
			.topics()
			.getList(databaseName)
			.stream()
			.filter(t -> !TopicName.get(t).isPartitioned()).collect(Collectors.toList());
	}

	public TableSchema getTableSchema(ObjectPath objectPath) throws PulsarAdminException {
		String topicName = objectPath2TopicName(objectPath);
		FieldsDataType fieldsDataType = getSchema(topicName);
		return SchemaUtils.toTableSchema(fieldsDataType);
	}

	public boolean topicExists(ObjectPath objectPath) throws PulsarAdminException {
		String topicName = objectPath2TopicName(objectPath);
		int partitionNum = admin.topics().getPartitionedTopicMetadata(topicName).partitions;
		if (partitionNum > 0) {
			return true;
		} else {
			admin.topics().getStats(topicName);
		}
		return true;
	}

	public void deleteTopic(ObjectPath objectPath) throws PulsarAdminException {
		String topicName = objectPath2TopicName(objectPath);
		int partitionNum = admin.topics().getPartitionedTopicMetadata(topicName).partitions;
		if (partitionNum > 0) {
			admin.topics().deletePartitionedTopic(topicName, true);
		} else {
			admin.topics().delete(topicName, true);
		}
	}

	public void createTopic(ObjectPath objectPath, int defaultPartitionNum, CatalogBaseTable table) throws PulsarAdminException, SchemaUtils.IncompatibleSchemaException {
		String topicName = objectPath2TopicName(objectPath);
		admin.topics().createPartitionedTopic(topicName, defaultPartitionNum);
		SchemaInfo si = SchemaUtils.sqlType2PulsarSchema(table.getSchema().toRowDataType()).getSchemaInfo();
	}

	private FieldsDataType getSchema(String topic) throws PulsarAdminException {
		SchemaInfo si = getPulsarSchema(topic);
		try {
			return SchemaUtils.pulsarSourceSchema(si);
		} catch (SchemaUtils.IncompatibleSchemaException e) {
			throw new PulsarAdminException(e);
		}
	}

	private SchemaInfo getPulsarSchema(String topic) throws PulsarAdminException {
		try {
			return admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
		} catch (PulsarAdminException e) {
			if (e.getStatusCode() == 404) {
				return BytesSchema.of().getSchemaInfo();
			} else {
				throw e;
			}
		}
	}

	private static String objectPath2TopicName(ObjectPath objectPath) {
		NamespaceName ns = NamespaceName.get(objectPath.getDatabaseName());
		String topic = objectPath.getObjectName();
		TopicName fullName = TopicName.get(TopicDomain.persistent.toString(), ns, topic);
		return fullName.toString();
	}



}
