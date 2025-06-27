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

package org.apache.flink.streaming.api.operators.python.process.timer;

import java.util.List;

/** {@link TimerRegistrationAction} used to register Timer. */
public class TimerRegistrationAction {

    private final TimerRegistration timerRegistration;

    private final byte[] serializedTimerData;

    private boolean isRegistered;

    private final List<TimerRegistrationAction> containingList;

    public TimerRegistrationAction(
            TimerRegistration timerRegistration,
            byte[] serializedTimerData,
            List<TimerRegistrationAction> containingList) {
        this.timerRegistration = timerRegistration;
        this.serializedTimerData = serializedTimerData;
        this.containingList = containingList;
        this.isRegistered = false;
    }

    public void run() {
        registerTimer();
        cleanup();
    }

    public void registerTimer() {
        if (!isRegistered) {
            timerRegistration.setTimer(serializedTimerData);
            isRegistered = true;
        }
    }

    private void cleanup() {
        if (isRegistered) {
            containingList.remove(this);
        }
    }
}
