/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.compression.profiling;

import eu.stratosphere.nephele.protocols.VersionedProtocol;
import eu.stratosphere.nephele.types.IntegerRecord;

public interface ThroughputAnalyzerProtocol extends VersionedProtocol {

	void reportDatapackageSend(IntegerRecord compressorID, IntegerRecord dataID, IntegerRecord bytes,
			IntegerRecord uncompressedBytes, IntegerRecord uncompressedBufferSize, IntegerRecord compressionlevel);

	void reportDatapackageReceive(IntegerRecord dataID);

	ThroughputAnalyzerResult getAverageCommunicationTimeForCompressor(IntegerRecord compressorID);
}
