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

package org.apache.flink.walkthrough.common.entity;

import java.util.Objects;

/**
 * A simple transaction.
 */
@SuppressWarnings("unused")
public final class Transaction {

	private long accountId;

	private long timestamp;

	private double amount;

	public Transaction() { }

	public Transaction(long accountId, long timestamp, double amount) {
		this.accountId = accountId;
		this.timestamp = timestamp;
		this.amount = amount;
	}

	public long getAccountId() {
		return accountId;
	}

	public void setAccountId(long accountId) {
		this.accountId = accountId;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public double getAmount() {
		return amount;
	}

	public void setAmount(double amount) {
		this.amount = amount;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		} else if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Transaction that = (Transaction) o;
		return accountId == that.accountId &&
			timestamp == that.timestamp &&
			Double.compare(that.amount, amount) == 0;
	}

	@Override
	public int hashCode() {
		return Objects.hash(accountId, timestamp, amount);
	}

	@Override
	public String toString() {
		return "Transaction{" +
			"accountId=" + accountId +
			", timestamp=" + timestamp +
			", amount=" + amount +
			'}';
	}
}
