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

package org.apache.flink.connector.pulsar.common.utils;

import org.apache.flink.annotation.Internal;

import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.transaction.TransactionImpl;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Transaction was introduced into pulsar since 2.7.0, but the interface {@link Transaction} didn't
 * provide a id method until 2.8.1. We have to add this util for acquiring the {@link TxnID} for
 * compatible consideration.
 *
 * <p>TODO Remove this hack after pulsar 2.8.1 release.
 */
@Internal
@SuppressWarnings("java:S3011")
public final class PulsarTransactionUtils {

    private static volatile Field mostBitsField;
    private static volatile Field leastBitsField;

    private PulsarTransactionUtils() {
        // No public constructor
    }

    public static TxnID getId(Transaction transaction) {
        // 2.8.1 and after.
        try {
            Method getId = Transaction.class.getDeclaredMethod("getTxnID");
            return (TxnID) getId.invoke(transaction);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            // 2.8.0 and before.
            TransactionImpl impl = (TransactionImpl) transaction;
            Long txnIdMostBits = getTxnIdMostBits(impl);
            Long txnIdLeastBits = getTxnIdLeastBits(impl);

            checkNotNull(txnIdMostBits, "Failed to get txnIdMostBits");
            checkNotNull(txnIdLeastBits, "Failed to get txnIdLeastBits");

            return new TxnID(txnIdMostBits, txnIdLeastBits);
        }
    }

    private static Long getTxnIdMostBits(TransactionImpl transaction) {
        if (mostBitsField == null) {
            synchronized (PulsarTransactionUtils.class) {
                if (mostBitsField == null) {
                    try {
                        mostBitsField = TransactionImpl.class.getDeclaredField("txnIdMostBits");
                        mostBitsField.setAccessible(true);
                    } catch (NoSuchFieldException e) {
                        // Nothing to do for this exception.
                    }
                }
            }
        }

        if (mostBitsField != null) {
            try {
                return (Long) mostBitsField.get(transaction);
            } catch (IllegalAccessException e) {
                // Nothing to do for this exception.
            }
        }

        return null;
    }

    private static Long getTxnIdLeastBits(TransactionImpl transaction) {
        if (leastBitsField == null) {
            synchronized (PulsarTransactionUtils.class) {
                if (leastBitsField == null) {
                    try {
                        leastBitsField = TransactionImpl.class.getDeclaredField("txnIdLeastBits");
                        leastBitsField.setAccessible(true);
                    } catch (NoSuchFieldException e) {
                        // Nothing to do for this exception.
                    }
                }
            }
        }

        if (leastBitsField != null) {
            try {
                return (Long) leastBitsField.get(transaction);
            } catch (IllegalAccessException e) {
                // Nothing to do for this exception.
            }
        }

        return null;
    }
}
