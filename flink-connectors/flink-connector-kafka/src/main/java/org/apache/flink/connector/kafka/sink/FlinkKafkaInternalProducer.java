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

package org.apache.flink.connector.kafka.sink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.internals.TransactionManager;
import org.apache.kafka.clients.producer.internals.TransactionalRequestResult;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link KafkaProducer} that exposes private fields to allow resume producing from a given state.
 */
class FlinkKafkaInternalProducer<K, V> extends KafkaProducer<K, V> {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaInternalProducer.class);
    private static final String TRANSACTION_MANAGER_FIELD_NAME = "transactionManager";
    private static final String TRANSACTION_MANAGER_STATE_ENUM =
            "org.apache.kafka.clients.producer.internals.TransactionManager$State";
    private static final String PRODUCER_ID_AND_EPOCH_FIELD_NAME = "producerIdAndEpoch";

    @Nullable private String transactionalId;
    private volatile boolean inTransaction;
    private volatile boolean closed;

    public FlinkKafkaInternalProducer(Properties properties, @Nullable String transactionalId) {
        super(withTransactionalId(properties, transactionalId));
        this.transactionalId = transactionalId;
    }

    private static Properties withTransactionalId(
            Properties properties, @Nullable String transactionalId) {
        if (transactionalId == null) {
            return properties;
        }
        Properties props = new Properties();
        props.putAll(properties);
        props.setProperty(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        return props;
    }

    @Override
    public void flush() {
        super.flush();
        if (inTransaction) {
            flushNewPartitions();
        }
    }

    @Override
    public void beginTransaction() throws ProducerFencedException {
        super.beginTransaction();
        inTransaction = true;
    }

    @Override
    public void abortTransaction() throws ProducerFencedException {
        LOG.debug("abortTransaction {}", transactionalId);
        checkState(inTransaction, "Transaction was not started");
        inTransaction = false;
        super.abortTransaction();
    }

    @Override
    public void commitTransaction() throws ProducerFencedException {
        LOG.debug("commitTransaction {}", transactionalId);
        checkState(inTransaction, "Transaction was not started");
        inTransaction = false;
        super.commitTransaction();
    }

    public boolean isInTransaction() {
        return inTransaction;
    }

    @Override
    public void close() {
        closed = true;
        if (inTransaction) {
            // This is state is most likely reached in case of a failure.
            // If this producer is still in transaction, it should be committing.
            // However, at this point, we cannot decide that and we shouldn't prolong cancellation.
            // So hard kill this producer with all resources.
            super.close(Duration.ZERO);
        } else {
            // If this is outside of a transaction, we should be able to cleanly shutdown.
            super.close(Duration.ofHours(1));
        }
    }

    @Override
    public void close(Duration timeout) {
        closed = true;
        super.close(timeout);
    }

    @Override
    public void close(long timeout, TimeUnit unit) {
        closed = true;
        super.close(timeout, unit);
    }

    public boolean isClosed() {
        return closed;
    }

    @Nullable
    public String getTransactionalId() {
        return transactionalId;
    }

    public short getEpoch() {
        Object transactionManager = getTransactionManager();
        Object producerIdAndEpoch = getField(transactionManager, PRODUCER_ID_AND_EPOCH_FIELD_NAME);
        return (short) getField(producerIdAndEpoch, "epoch");
    }

    public long getProducerId() {
        Object transactionManager = getTransactionManager();
        Object producerIdAndEpoch = getField(transactionManager, PRODUCER_ID_AND_EPOCH_FIELD_NAME);
        return (long) getField(producerIdAndEpoch, "producerId");
    }

    public void initTransactionId(String transactionalId) {
        if (!transactionalId.equals(this.transactionalId)) {
            setTransactionId(transactionalId);
            initTransactions();
        }
    }

    public void setTransactionId(String transactionalId) {
        if (!transactionalId.equals(this.transactionalId)) {
            checkState(
                    !inTransaction,
                    String.format("Another transaction %s is still open.", transactionalId));
            LOG.debug("Change transaction id from {} to {}", this.transactionalId, transactionalId);
            Object transactionManager = getTransactionManager();
            synchronized (transactionManager) {
                setField(transactionManager, "transactionalId", transactionalId);
                setField(
                        transactionManager,
                        "currentState",
                        getTransactionManagerState("UNINITIALIZED"));
                this.transactionalId = transactionalId;
            }
        }
    }

    /**
     * Besides committing {@link org.apache.kafka.clients.producer.KafkaProducer#commitTransaction}
     * is also adding new partitions to the transaction. flushNewPartitions method is moving this
     * logic to pre-commit/flush, to make resumeTransaction simpler. Otherwise resumeTransaction
     * would require to restore state of the not yet added/"in-flight" partitions.
     */
    private void flushNewPartitions() {
        LOG.info("Flushing new partitions");
        TransactionalRequestResult result = enqueueNewPartitions();
        Object sender = getField("sender");
        invoke(sender, "wakeup");
        result.await();
    }

    /**
     * Enqueues new transactions at the transaction manager and returns a {@link
     * TransactionalRequestResult} that allows waiting on them.
     *
     * <p>If there are no new transactions we return a {@link TransactionalRequestResult} that is
     * already done.
     */
    private TransactionalRequestResult enqueueNewPartitions() {
        Object transactionManager = getTransactionManager();
        synchronized (transactionManager) {
            Object newPartitionsInTransaction =
                    getField(transactionManager, "newPartitionsInTransaction");
            Object newPartitionsInTransactionIsEmpty =
                    invoke(newPartitionsInTransaction, "isEmpty");
            TransactionalRequestResult result;
            if (newPartitionsInTransactionIsEmpty instanceof Boolean
                    && !((Boolean) newPartitionsInTransactionIsEmpty)) {
                Object txnRequestHandler =
                        invoke(transactionManager, "addPartitionsToTransactionHandler");
                invoke(
                        transactionManager,
                        "enqueueRequest",
                        new Class[] {txnRequestHandler.getClass().getSuperclass()},
                        new Object[] {txnRequestHandler});
                result =
                        (TransactionalRequestResult)
                                getField(
                                        txnRequestHandler,
                                        txnRequestHandler.getClass().getSuperclass(),
                                        "result");
            } else {
                // we don't have an operation but this operation string is also used in
                // addPartitionsToTransactionHandler.
                result = new TransactionalRequestResult("AddPartitionsToTxn");
                result.done();
            }
            return result;
        }
    }

    private static Object invoke(Object object, String methodName, Object... args) {
        Class<?>[] argTypes = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].getClass();
        }
        return invoke(object, methodName, argTypes, args);
    }

    private static Object invoke(
            Object object, String methodName, Class<?>[] argTypes, Object[] args) {
        try {
            Method method = object.getClass().getDeclaredMethod(methodName, argTypes);
            method.setAccessible(true);
            return method.invoke(object, args);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    private Object getField(String fieldName) {
        return getField(this, KafkaProducer.class, fieldName);
    }

    /**
     * Gets and returns the field {@code fieldName} from the given Object {@code object} using
     * reflection.
     */
    private static Object getField(Object object, String fieldName) {
        return getField(object, object.getClass(), fieldName);
    }

    /**
     * Gets and returns the field {@code fieldName} from the given Object {@code object} using
     * reflection.
     */
    private static Object getField(Object object, Class<?> clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return field.get(object);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    /**
     * Instead of obtaining producerId and epoch from the transaction coordinator, re-use previously
     * obtained ones, so that we can resume transaction after a restart. Implementation of this
     * method is based on {@link KafkaProducer#initTransactions}.
     * https://github.com/apache/kafka/commit/5d2422258cb975a137a42a4e08f03573c49a387e#diff-f4ef1afd8792cd2a2e9069cd7ddea630
     */
    public void resumeTransaction(long producerId, short epoch) {
        checkState(!inTransaction, "Already in transaction %s", transactionalId);
        checkState(
                producerId >= 0 && epoch >= 0,
                "Incorrect values for producerId %s and epoch %s",
                producerId,
                epoch);
        LOG.info(
                "Attempting to resume transaction {} with producerId {} and epoch {}",
                transactionalId,
                producerId,
                epoch);

        Object transactionManager = getTransactionManager();
        synchronized (transactionManager) {
            Object topicPartitionBookkeeper =
                    getField(transactionManager, "topicPartitionBookkeeper");

            transitionTransactionManagerStateTo(transactionManager, "INITIALIZING");
            invoke(topicPartitionBookkeeper, "reset");

            setField(
                    transactionManager,
                    PRODUCER_ID_AND_EPOCH_FIELD_NAME,
                    createProducerIdAndEpoch(producerId, epoch));

            transitionTransactionManagerStateTo(transactionManager, "READY");

            transitionTransactionManagerStateTo(transactionManager, "IN_TRANSACTION");
            setField(transactionManager, "transactionStarted", true);
            this.inTransaction = true;
        }
    }

    private static Object createProducerIdAndEpoch(long producerId, short epoch) {
        try {
            Field field =
                    TransactionManager.class.getDeclaredField(PRODUCER_ID_AND_EPOCH_FIELD_NAME);
            Class<?> clazz = field.getType();
            Constructor<?> constructor = clazz.getDeclaredConstructor(Long.TYPE, Short.TYPE);
            constructor.setAccessible(true);
            return constructor.newInstance(producerId, epoch);
        } catch (InvocationTargetException
                | InstantiationException
                | IllegalAccessException
                | NoSuchFieldException
                | NoSuchMethodException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    /**
     * Sets the field {@code fieldName} on the given Object {@code object} to {@code value} using
     * reflection.
     */
    private static void setField(Object object, String fieldName, Object value) {
        setField(object, object.getClass(), fieldName, value);
    }

    private static void setField(Object object, Class<?> clazz, String fieldName, Object value) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            field.set(object, value);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Enum<?> getTransactionManagerState(String enumName) {
        try {
            Class<Enum> cl = (Class<Enum>) Class.forName(TRANSACTION_MANAGER_STATE_ENUM);
            return Enum.valueOf(cl, enumName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Incompatible KafkaProducer version", e);
        }
    }

    private Object getTransactionManager() {
        return getField(TRANSACTION_MANAGER_FIELD_NAME);
    }

    private static void transitionTransactionManagerStateTo(
            Object transactionManager, String state) {
        invoke(transactionManager, "transitionTo", getTransactionManagerState(state));
    }

    @Override
    public String toString() {
        return "FlinkKafkaInternalProducer{"
                + "transactionalId='"
                + transactionalId
                + "', inTransaction="
                + inTransaction
                + ", closed="
                + closed
                + '}';
    }
}
