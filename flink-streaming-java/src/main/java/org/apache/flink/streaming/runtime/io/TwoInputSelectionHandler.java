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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.InputSelectable;
import org.apache.flink.streaming.api.operators.InputSelection;

import javax.annotation.Nullable;

/**
 * This handler is mainly used for selecting the next available input index in {@link
 * StreamTwoInputProcessor}.
 */
@Internal
public class TwoInputSelectionHandler {

    @Nullable private final InputSelectable inputSelectable;

    private int selectedInputsMask = (int) InputSelection.ALL.getInputMask();

    private int availableInputsMask;
    private final int allInputsMask = 3;

    private int dataFinishedButNotPartition = 0;

    private int inputsFinishedMask = 0;

    public TwoInputSelectionHandler(@Nullable InputSelectable inputSelectable) {
        this.inputSelectable = inputSelectable;
        this.availableInputsMask =
                (int) new InputSelection.Builder().select(1).select(2).build().getInputMask();
    }

    void nextSelection() {
        if (inputSelectable == null) {
            selectedInputsMask = (int) InputSelection.ALL.getInputMask();
        } else if (dataFinishedButNotPartition != 0) {
            selectedInputsMask =
                    ((int) inputSelectable.nextSelection().getInputMask()
                            | dataFinishedButNotPartition);
        } else {
            selectedInputsMask = (int) inputSelectable.nextSelection().getInputMask();
        }
    }

    public boolean allInputsReceivedEndOfData() {
        return (dataFinishedButNotPartition | inputsFinishedMask) == 3;
    }

    int selectNextInputIndex(int lastReadInputIndex) {
        return InputSelection.fairSelectNextIndexOutOf2(
                selectedInputsMask, availableInputsMask, lastReadInputIndex);
    }

    boolean shouldSetAvailableForAnotherInput() {
        return availableInputsMask < 3 && areAllInputsSelected();
    }

    void setAvailableInput(int inputIndex) {
        availableInputsMask |= 1 << inputIndex;
    }

    void setUnavailableInput(int inputIndex) {
        availableInputsMask &= ~(1 << inputIndex);
    }

    void setDataFinishedOnInput(int inputIndex) {
        dataFinishedButNotPartition |= 1 << inputIndex;
    }

    boolean areAllInputsSelected() {
        return (selectedInputsMask & allInputsMask) == 3;
    }

    boolean isFirstInputSelected() {
        return checkBitMask(selectedInputsMask, 0);
    }

    boolean isSecondInputSelected() {
        return checkBitMask(selectedInputsMask, 1);
    }

    public void setEndOfPartition(int inputIndex) {
        dataFinishedButNotPartition &= ~(1L << inputIndex);
        inputsFinishedMask |= 1 << inputIndex;
    }

    boolean checkBitMask(long mask, int inputIndex) {
        return (mask & (1L << inputIndex)) != 0;
    }
}
