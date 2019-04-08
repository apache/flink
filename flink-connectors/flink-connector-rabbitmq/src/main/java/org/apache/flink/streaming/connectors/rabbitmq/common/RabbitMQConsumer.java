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

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AlreadyClosedException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.ShutdownSignalException;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Blocking Rabbit MQ Consumer.
 * Automatically writes data to an internal buffer.
 * If RabbitMQ fills the buffer the connection is killed until the buffer is emptied
 */
public class RabbitMQConsumer extends DefaultConsumer {

	private final BlockingQueue<Delivery> buffer;

	private final String queue;
	private final boolean autoAck;
	private volatile AtomicBoolean cancelled = new AtomicBoolean(false);
	private volatile AtomicBoolean shutdown = new AtomicBoolean(false);

	public RabbitMQConsumer(String queue, boolean autoAck, Channel ch) {
		this(queue, autoAck, ch, new LinkedBlockingQueue<>(Integer.MAX_VALUE));
	}

	public RabbitMQConsumer(String queue, int capacity, boolean autoAck, Channel ch) {
		this(queue, autoAck, ch, new LinkedBlockingQueue<>(capacity));
	}

	public RabbitMQConsumer(String queue, boolean autoAck, Channel ch, BlockingQueue<Delivery> buffer) {
		super(ch);
		this.queue = queue;
		this.buffer = buffer;
		this.autoAck = autoAck;
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope, BasicProperties properties, byte[] body) {
		try {
			boolean hasBeenAdded = buffer.offer(new Delivery(envelope, properties, body), 1, TimeUnit.SECONDS);
			while (!hasBeenAdded || shutdown.get()) {
				hasBeenAdded = buffer.offer(new Delivery(envelope, properties, body), 1, TimeUnit.SECONDS);
			}
		} catch (InterruptedException e) {
			handleDelivery(consumerTag, envelope, properties, body);
		}
	}

	@Override
	public void handleCancel(String consumerTag) throws IOException {
		super.handleCancel(consumerTag);
		cancelled.set(true);
	}

	@Override
	public void handleCancelOk(String consumerTag) {
		super.handleCancelOk(consumerTag);
		cancelled.set(true);
	}

	@Override
	public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
		super.handleShutdownSignal(consumerTag, sig);
		shutdown.set(true);
		cancelled.set(true);
	}

	@Override
	public void handleConsumeOk(String consumerTag) {
		super.handleConsumeOk(consumerTag);
		cancelled.set(false);
	}

	@Override
	public void handleRecoverOk(String consumerTag) {
		super.handleRecoverOk(consumerTag);
		cancelled.set(false);
	}

	public Delivery nextDelivery(long timeout, TimeUnit unit)
		throws InterruptedException, ShutdownSignalException, ConsumerCancelledException, IOException {
		if (cancelled.get() && !shutdown.get() && buffer.remainingCapacity() > buffer.size() / 2) {
			try {
				getChannel().basicConsume(queue, autoAck, this.getConsumerTag(), this);
			} catch (AlreadyClosedException ignored) {
					/*Auto-recovery mechanisms can cause errors
					because there is no concurrency handling around channel actions */
				nextDelivery(timeout, unit);
			} catch (IOException e) {
				if (!(e.getCause() instanceof ShutdownSignalException)) {
					throw e;
				}
			}
			cancelled.set(false);
		} else if (buffer.remainingCapacity() == 0 && !cancelled.get()) {
			try {
				getChannel().basicCancel(this.getConsumerTag());
			} catch (IOException e) {
				if (!(e.getCause() instanceof ShutdownSignalException)) {
					throw e;
				}
			} catch (AlreadyClosedException ignored) {
				/* Auto-recovery mechanisms can cause errors
					because there is no concurrency handling around channel actions */
				nextDelivery(timeout, unit);
			}
			cancelled.set(true);
		}
		return buffer.poll(timeout, unit);
	}
}

