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

package org.apache.flink.mesos.scheduler.messages;

import org.apache.mesos.Protos;

import java.io.Serializable;
import java.util.Collection;

/** Local message sent by the launch coordinator to the scheduler to accept offers. */
public class AcceptOffers implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String hostname;
    private final Collection<Protos.OfferID> offerIds;
    private final Collection<Protos.Offer.Operation> operations;
    private final Protos.Filters filters;

    public AcceptOffers(
            String hostname,
            Collection<Protos.OfferID> offerIds,
            Collection<Protos.Offer.Operation> operations) {
        this.hostname = hostname;
        this.offerIds = offerIds;
        this.operations = operations;
        this.filters = Protos.Filters.newBuilder().build();
    }

    public AcceptOffers(
            String hostname,
            Collection<Protos.OfferID> offerIds,
            Collection<Protos.Offer.Operation> operations,
            Protos.Filters filters) {
        this.hostname = hostname;
        this.offerIds = offerIds;
        this.operations = operations;
        this.filters = filters;
    }

    public String hostname() {
        return hostname;
    }

    public Collection<Protos.OfferID> offerIds() {
        return offerIds;
    }

    public Collection<Protos.Offer.Operation> operations() {
        return operations;
    }

    public Protos.Filters filters() {
        return filters;
    }

    @Override
    public String toString() {
        return "AcceptOffers{"
                + "hostname='"
                + hostname
                + '\''
                + ", offerIds="
                + offerIds
                + ", operations="
                + operations
                + ", filters="
                + filters
                + '}';
    }
}
