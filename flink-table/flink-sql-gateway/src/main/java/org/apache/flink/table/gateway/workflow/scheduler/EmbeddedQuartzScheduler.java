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

package org.apache.flink.table.gateway.workflow.scheduler;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.gateway.SqlGateway;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.rest.header.materializedtable.RefreshMaterializedTableHeaders;
import org.apache.flink.table.gateway.rest.header.operation.CloseOperationHeaders;
import org.apache.flink.table.gateway.rest.header.session.CloseSessionHeaders;
import org.apache.flink.table.gateway.rest.header.session.OpenSessionHeaders;
import org.apache.flink.table.gateway.rest.header.statement.FetchResultsHeaders;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableParameters;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableRequestBody;
import org.apache.flink.table.gateway.rest.message.materializedtable.RefreshMaterializedTableResponseBody;
import org.apache.flink.table.gateway.rest.message.operation.OperationMessageParameters;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionRequestBody;
import org.apache.flink.table.gateway.rest.message.session.OpenSessionResponseBody;
import org.apache.flink.table.gateway.rest.message.session.SessionMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsMessageParameters;
import org.apache.flink.table.gateway.rest.message.statement.FetchResultsResponseBody;
import org.apache.flink.table.gateway.rest.message.statement.NotReadyFetchResultResponse;
import org.apache.flink.table.gateway.rest.util.RowFormat;
import org.apache.flink.table.gateway.rest.util.SqlGatewayRestEndpointUtils;
import org.apache.flink.table.gateway.workflow.WorkflowInfo;
import org.apache.flink.util.concurrent.Executors;

import org.quartz.CronTrigger;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.TriggerKey;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.WORKFLOW_INFO;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.dateToString;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.fromJson;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.getJobKey;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.initializeQuartzSchedulerConfig;
import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.toJson;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

/**
 * An embedded workflow scheduler based on quartz {@link Scheduler} that store all workflow in
 * memory, it does not have high availability. This scheduler will be embedded in {@link SqlGateway}
 * process to provide service by rest api.
 *
 * <p>This embedded scheduler is mainly used for testing scenarios and is not suitable for
 * production environment.
 */
@Internal
public class EmbeddedQuartzScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(EmbeddedQuartzScheduler.class);

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private Scheduler quartzScheduler;

    public void start() {
        Properties properties = initializeQuartzSchedulerConfig();
        try {
            quartzScheduler = new StdSchedulerFactory(properties).getScheduler();
            quartzScheduler.start();
            LOG.info("Start quartz scheduler successfully.");
        } catch (org.quartz.SchedulerException e) {
            String msg =
                    String.format("Failed to start quartz scheduler with config: %s.", properties);
            LOG.error(msg);
            throw new SchedulerException(msg, e);
        }
    }

    public void stop() {
        try {
            quartzScheduler.shutdown();
        } catch (org.quartz.SchedulerException e) {
            LOG.error("Failed to shutdown quartz schedule.");
            throw new SchedulerException("Failed to shutdown quartz scheduler.", e);
        }
    }

    public JobDetail createScheduleWorkflow(WorkflowInfo workflowInfo, String cronExpression)
            throws SchedulerException {
        String materializedTableIdentifier = workflowInfo.getMaterializedTableIdentifier();
        JobKey jobKey = getJobKey(materializedTableIdentifier);

        lock.writeLock().lock();
        try {
            // check if the quartz schedule job is exists firstly.
            if (quartzScheduler.checkExists(jobKey)) {
                LOG.error(
                        "Materialized table {} quartz schedule job already exist, job info: {}.",
                        materializedTableIdentifier,
                        jobKey);
                throw new SchedulerException(
                        String.format(
                                "Materialized table %s quartz schedule job already exist, job info: %s.",
                                materializedTableIdentifier, jobKey));
            }

            JobDetail jobDetail =
                    JobBuilder.newJob(EmbeddedSchedulerJob.class).withIdentity(jobKey).build();
            // put workflow info to job data map
            jobDetail.getJobDataMap().put(WORKFLOW_INFO, toJson(workflowInfo));

            TriggerKey triggerKey = TriggerKey.triggerKey(jobKey.getName(), jobKey.getGroup());
            CronTrigger cronTrigger =
                    newTrigger()
                            .withIdentity(triggerKey)
                            .withSchedule(
                                    cronSchedule(cronExpression)
                                            .withMisfireHandlingInstructionIgnoreMisfires())
                            .forJob(jobDetail)
                            .build();

            // Create job in quartz scheduler with cron trigger
            quartzScheduler.scheduleJob(jobDetail, cronTrigger);
            LOG.info(
                    "Create quartz schedule job for materialized table {} successfully, job info: {}, cron expression: {}.",
                    materializedTableIdentifier,
                    jobKey,
                    cronExpression);

            return jobDetail;
        } catch (org.quartz.SchedulerException e) {
            LOG.error(
                    "Failed to create quartz schedule job for materialized table {}.",
                    materializedTableIdentifier,
                    e);
            throw new SchedulerException(
                    String.format(
                            "Failed to create quartz schedule job for materialized table %s.",
                            materializedTableIdentifier),
                    e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void suspendScheduleWorkflow(String workflowName, String workflowGroup)
            throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(workflowName, workflowGroup);
        lock.writeLock().lock();
        try {
            String errorMsg =
                    String.format(
                            "Failed to suspend a non-existent quartz schedule job: %s.", jobKey);
            checkJobExists(jobKey, errorMsg);

            quartzScheduler.pauseJob(jobKey);
        } catch (org.quartz.SchedulerException e) {
            LOG.error("Failed to suspend quartz schedule job: {}.", jobKey, e);
            throw new SchedulerException(
                    String.format("Failed to suspend quartz schedule job: %s.", jobKey), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Resume a quartz schedule job with new dynamic options. If the dynamic options is empty, just
     * resume the job. If the dynamic options is not empty, since we cannot update the old workflow
     * job, we need to remove the old job and create a new job with new dynamic options. The new job
     * will be with new dynamic options. The new job will use the same job key and cron expression
     * as the old job.
     *
     * @param workflowName The name of the workflow to be resumed.
     * @param workflowGroup The group of the workflow to be resumed.
     * @param dynamicOptions A map containing the new dynamic options for the workflow. If empty,
     *     the workflow is simply resumed.
     * @throws SchedulerException if the workflow does not exist or if there is an error resuming
     *     the workflow.
     */
    public void resumeScheduleWorkflow(
            String workflowName, String workflowGroup, Map<String, String> dynamicOptions)
            throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(workflowName, workflowGroup);
        lock.writeLock().lock();
        try {
            String errorMsg =
                    String.format(
                            "Failed to resume a non-existent quartz schedule job: %s.", jobKey);
            checkJobExists(jobKey, errorMsg);

            if (dynamicOptions.isEmpty()) {
                quartzScheduler.resumeJob(jobKey);
            } else {
                // remove old job and create a new job with new dynamic options
                JobDetail jobDetail = quartzScheduler.getJobDetail(jobKey);
                WorkflowInfo workflowInfo =
                        fromJson(
                                jobDetail.getJobDataMap().getString(WORKFLOW_INFO),
                                WorkflowInfo.class);
                // create a new job with new dynamic options
                WorkflowInfo newWorkflowInfo =
                        new WorkflowInfo(
                                workflowInfo.getMaterializedTableIdentifier(),
                                dynamicOptions,
                                workflowInfo.getInitConfig(),
                                workflowInfo.getExecutionConfig(),
                                workflowInfo.getRestEndpointUrl());

                // create a new job
                CronTrigger trigger =
                        (CronTrigger)
                                quartzScheduler.getTrigger(
                                        TriggerKey.triggerKey(jobKey.getName(), jobKey.getGroup()));
                String cronExpression = trigger.getCronExpression();

                // remove the old job
                quartzScheduler.deleteJob(jobKey);

                // schedule the new job
                createScheduleWorkflow(newWorkflowInfo, cronExpression);
            }
        } catch (org.quartz.SchedulerException e) {
            LOG.error("Failed to resume quartz schedule job: {}.", jobKey, e);
            throw new SchedulerException(
                    String.format("Failed to resume quartz schedule job: %s.", jobKey), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void deleteScheduleWorkflow(String workflowName, String workflowGroup)
            throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(workflowName, workflowGroup);
        lock.writeLock().lock();
        try {
            quartzScheduler.deleteJob(jobKey);
        } catch (org.quartz.SchedulerException e) {
            LOG.error("Failed to delete quartz schedule job: {}.", jobKey, e);
            throw new SchedulerException(
                    String.format("Failed to delete quartz schedule job: %s.", jobKey), e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void checkJobExists(JobKey jobKey, String errorMsg)
            throws org.quartz.SchedulerException {
        if (!quartzScheduler.checkExists(jobKey)) {
            LOG.error(errorMsg);
            throw new SchedulerException(errorMsg);
        }
    }

    @VisibleForTesting
    public Scheduler getQuartzScheduler() {
        return quartzScheduler;
    }

    /** The {@link Job} implementation for embedded quartz scheduler. */
    public static class EmbeddedSchedulerJob implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {

            SessionHandle sessionHandle = null;
            OperationHandle operationHandle = null;
            SqlGatewayRestClient gatewayRestClient = null;
            try {
                JobDataMap dataMap = context.getJobDetail().getJobDataMap();
                String workflowJsonStr = dataMap.getString(WORKFLOW_INFO);
                WorkflowInfo workflowInfo = fromJson(workflowJsonStr, WorkflowInfo.class);
                LOG.info("Execute refresh operation for workflow: {}.", workflowInfo);

                String schedulerTime = dateToString(context.getScheduledFireTime());

                gatewayRestClient = new SqlGatewayRestClient(workflowInfo.getRestEndpointUrl());
                sessionHandle =
                        gatewayRestClient.openSession(
                                String.format(
                                        "%s-quartz-refresh-session-%s",
                                        workflowInfo.getMaterializedTableIdentifier(),
                                        schedulerTime),
                                workflowInfo.getInitConfig());
                operationHandle =
                        gatewayRestClient.refreshMaterializedTable(
                                sessionHandle,
                                workflowInfo.getMaterializedTableIdentifier(),
                                schedulerTime,
                                workflowInfo.getDynamicOptions(),
                                Collections.emptyMap(),
                                workflowInfo.getExecutionConfig());

                List<RowData> results =
                        gatewayRestClient.fetchOperationAllResults(sessionHandle, operationHandle);

                String jobId = results.get(0).getString(0).toString();
                LOG.info(
                        "Successfully execute refresh operation for materialized table: {} with job id: {}.",
                        workflowInfo.getMaterializedTableIdentifier(),
                        jobId);

                context.setResult(
                        "Successfully execute refresh operation for materialized table: "
                                + workflowInfo.getMaterializedTableIdentifier()
                                + " with job id: "
                                + jobId);
                // TODO wait for the job to finish
            } catch (Exception e) {
                LOG.error("Failed to execute refresh operation for workflow.", e);
                throw new JobExecutionException(e.getMessage(), e);
            } finally {
                try {
                    if (gatewayRestClient != null) {
                        if (operationHandle != null) {
                            gatewayRestClient.closeOperation(sessionHandle, operationHandle);
                        }
                        if (sessionHandle != null) {
                            gatewayRestClient.closeSession(sessionHandle);
                        }
                        gatewayRestClient.close();
                    }
                } catch (Exception e) {
                    LOG.error("Failed to close session.", e);
                }
            }
        }

        /** A simple rest client for gateway rest endpoint. */
        private static class SqlGatewayRestClient implements AutoCloseable {

            private final String address;
            private final int port;
            private final RestClient restClient;

            private SqlGatewayRestClient(String endpointUrl) throws Exception {
                URL url = new URL(endpointUrl);
                this.address = url.getHost();
                this.port = url.getPort();
                this.restClient =
                        RestClient.forUrl(new Configuration(), Executors.directExecutor(), url);
            }

            private SessionHandle openSession(String sessionName, Map<String, String> initConfig)
                    throws Exception {
                OpenSessionRequestBody requestBody =
                        new OpenSessionRequestBody(sessionName, initConfig);
                OpenSessionHeaders headers = OpenSessionHeaders.getInstance();

                OpenSessionResponseBody responseBody =
                        restClient
                                .sendRequest(
                                        address,
                                        port,
                                        headers,
                                        EmptyMessageParameters.getInstance(),
                                        requestBody)
                                .get();

                return new SessionHandle(UUID.fromString(responseBody.getSessionHandle()));
            }

            private void closeSession(SessionHandle sessionHandle) throws Exception {
                // Close session
                CloseSessionHeaders closeSessionHeaders = CloseSessionHeaders.getInstance();
                SessionMessageParameters sessionMessageParameters =
                        new SessionMessageParameters(sessionHandle);
                restClient
                        .sendRequest(
                                address,
                                port,
                                closeSessionHeaders,
                                sessionMessageParameters,
                                EmptyRequestBody.getInstance())
                        .get();
            }

            private void closeOperation(
                    SessionHandle sessionHandle, OperationHandle operationHandle) throws Exception {
                // Close operation
                CloseOperationHeaders closeOperationHeaders = CloseOperationHeaders.getInstance();
                OperationMessageParameters operationMessageParameters =
                        new OperationMessageParameters(sessionHandle, operationHandle);
                restClient
                        .sendRequest(
                                address,
                                port,
                                closeOperationHeaders,
                                operationMessageParameters,
                                EmptyRequestBody.getInstance())
                        .get();
            }

            private OperationHandle refreshMaterializedTable(
                    SessionHandle sessionHandle,
                    String materializedTableIdentifier,
                    String schedulerTime,
                    Map<String, String> dynamicOptions,
                    Map<String, String> staticPartitions,
                    Map<String, String> executionConfig)
                    throws Exception {

                RefreshMaterializedTableRequestBody requestBody =
                        new RefreshMaterializedTableRequestBody(
                                true,
                                schedulerTime,
                                dynamicOptions,
                                staticPartitions,
                                executionConfig);
                RefreshMaterializedTableHeaders headers =
                        RefreshMaterializedTableHeaders.getInstance();
                RefreshMaterializedTableParameters parameters =
                        new RefreshMaterializedTableParameters(
                                sessionHandle, materializedTableIdentifier);

                RefreshMaterializedTableResponseBody responseBody =
                        restClient
                                .sendRequest(address, port, headers, parameters, requestBody)
                                .get();

                return new OperationHandle(UUID.fromString(responseBody.getOperationHandle()));
            }

            private List<RowData> fetchOperationAllResults(
                    SessionHandle sessionHandle, OperationHandle operationHandle) throws Exception {
                Long token = 0L;
                List<RowData> results = new ArrayList<>();
                while (token != null) {
                    FetchResultsResponseBody responseBody =
                            fetchOperationResults(sessionHandle, operationHandle, token);
                    if (responseBody instanceof NotReadyFetchResultResponse) {
                        Thread.sleep(10);
                        continue;
                    }
                    responseBody.getNextResultUri();
                    results.addAll(responseBody.getResults().getData());
                    token = SqlGatewayRestEndpointUtils.parseToken(responseBody.getNextResultUri());
                }
                return results;
            }

            private FetchResultsResponseBody fetchOperationResults(
                    SessionHandle sessionHandle, OperationHandle operationHandle, Long token)
                    throws Exception {
                FetchResultsMessageParameters fetchResultsMessageParameters =
                        new FetchResultsMessageParameters(
                                sessionHandle, operationHandle, token, RowFormat.JSON);
                FetchResultsHeaders fetchResultsHeaders = FetchResultsHeaders.getDefaultInstance();
                CompletableFuture<FetchResultsResponseBody> response =
                        restClient.sendRequest(
                                address,
                                port,
                                fetchResultsHeaders,
                                fetchResultsMessageParameters,
                                EmptyRequestBody.getInstance());
                return response.get();
            }

            @Override
            public void close() {
                try {
                    restClient.close();
                } catch (Exception e) {
                    LOG.error("Failed to close rest client.", e);
                }
            }
        }
    }
}
