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
import org.apache.flink.table.gateway.SqlGateway;
import org.apache.flink.table.gateway.workflow.WorkflowInfo;

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

import java.util.Properties;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.flink.table.gateway.workflow.scheduler.QuartzSchedulerUtils.WORKFLOW_INFO;
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

    public void resumeScheduleWorkflow(String workflowName, String workflowGroup)
            throws SchedulerException {
        JobKey jobKey = JobKey.jobKey(workflowName, workflowGroup);
        lock.writeLock().lock();
        try {
            String errorMsg =
                    String.format(
                            "Failed to resume a non-existent quartz schedule job: %s.", jobKey);
            checkJobExists(jobKey, errorMsg);

            quartzScheduler.resumeJob(jobKey);
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
            String errorMsg =
                    String.format(
                            "Failed to delete a non-existent quartz schedule job: %s.", jobKey);
            checkJobExists(jobKey, errorMsg);

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

    /** The {@link Job} implementation for embedded quartz scheduler. */
    private class EmbeddedSchedulerJob implements Job {

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            JobDataMap dataMap = context.getJobDetail().getJobDataMap();
            String workflowJsonStr = dataMap.getString(WORKFLOW_INFO);
            WorkflowInfo workflowInfo = fromJson(workflowJsonStr, WorkflowInfo.class);
            // TODO: implement the refresh operation for materialized table, see FLINK-35348
            LOG.info("Execute refresh operation for workflow: {}.", workflowInfo);
        }
    }
}
