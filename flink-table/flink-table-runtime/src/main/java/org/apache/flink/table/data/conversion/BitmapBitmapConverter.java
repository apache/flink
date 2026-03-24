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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.types.bitmap.Bitmap;
import org.apache.flink.types.bitmap.RoaringBitmapData;

/** Converter for {@link Bitmap} type that only accepts {@link RoaringBitmapData} as input. */
@Internal
public class BitmapBitmapConverter implements DataStructureConverter<Bitmap, Bitmap> {

    private static final long serialVersionUID = 1L;

    @Override
    public Bitmap toInternal(Bitmap external) {
        if (!(external instanceof RoaringBitmapData)) {
            throw new UnsupportedOperationException(
                    "Unsupported bitmap type: " + external.getClass().getSimpleName() + ".");
        }
        return external;
    }

    @Override
    public Bitmap toExternal(Bitmap internal) {
        return internal;
    }
}
