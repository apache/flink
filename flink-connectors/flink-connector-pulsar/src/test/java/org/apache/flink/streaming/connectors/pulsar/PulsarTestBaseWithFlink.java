/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pulsar;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.pulsar.common.naming.TopicName;
import org.junit.Before;
import org.junit.ClassRule;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public abstract class PulsarTestBaseWithFlink extends PulsarTestBase {

    protected static final int NUM_TMS = 1;

    protected static final int TM_SLOTS = 8;

    protected ClusterClient<?> client;

    @ClassRule
    public static MiniClusterWithClientResource flink = new MiniClusterWithClientResource(
            new MiniClusterResourceConfiguration.Builder()
                    .setConfiguration(getFlinkConfiguration())
                    .setNumberTaskManagers(NUM_TMS)
                    .setNumberSlotsPerTaskManager(TM_SLOTS)
                    .build());

    @Before
    public void noJobIsRunning() throws Exception {
        client = flink.getClusterClient();
        waitUntilNoJobIsRunning(client);
    }

    public static void waitUntilJobIsRunning(ClusterClient<?> client) throws Exception {
        while (getRunningJobs(client).isEmpty()) {
            Thread.sleep(50);
        }
    }

    public static void waitUntilNoJobIsRunning(ClusterClient<?> client) throws Exception {
        while (!getRunningJobs(client).isEmpty()) {
            Thread.sleep(50);
            cancelRunningJobs(client);
        }
    }

    public static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
        Collection<JobStatusMessage> statusMessages = client.listJobs().get();
        return statusMessages.stream()
                .filter(status -> !status.getJobState().isGloballyTerminalState())
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toList());
    }

    public static void cancelRunningJobs(ClusterClient<?> client) throws Exception {
        List<JobID> runningJobs = getRunningJobs(client);
        for (JobID runningJob : runningJobs) {
            client.cancel(runningJob);
        }
    }

    public static final AtomicInteger topicId = new AtomicInteger(0);

    public static String newTopic() {
        synchronized (topicId) {
            int i = topicId.getAndIncrement();
            return TopicName.get("topic-" + i).toString();
        }
    }
}
