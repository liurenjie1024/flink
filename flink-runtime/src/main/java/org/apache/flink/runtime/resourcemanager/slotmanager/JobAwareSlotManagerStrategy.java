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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

public class JobAwareSlotManagerStrategy implements SlotManagerStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(JobAwareSlotManagerStrategy.class);

	private final Map<JobID, SlotManagementWorker> slotManagers;
	private final Map<InstanceID, JobID> taskManagers;
	private final Map<AllocationID, JobID> slotRequests;

	private final ScheduledExecutor scheduledExecutor;

	/** Timeout for slot requests to the task manager. */
	private final Time taskManagerRequestTimeout;

	/** Timeout after which an allocation is discarded. */
	private final Time slotRequestTimeout;

	/** Timeout after which an unused TaskManager is released. */
	private final Time taskManagerTimeout;

	/** Executor for future callbacks which have to be "synchronized". */
	private Executor mainThreadExecutor;

	private ScheduledFuture<?> taskManagerTimeoutCheck;

	private ScheduledFuture<?> slotRequestTimeoutCheck;

	private ResourceManagerId resourceManagerId;
	private ResourceActions resourceActions;

	private boolean started = false;

	public JobAwareSlotManagerStrategy(
		ScheduledExecutor scheduledExecutor,
		Time taskManagerRequestTimeout,
		Time slotRequestTimeout,
		Time taskManagerTimeout) {

		this.scheduledExecutor = scheduledExecutor;
		this.taskManagerRequestTimeout = taskManagerRequestTimeout;
		this.slotRequestTimeout = slotRequestTimeout;
		this.taskManagerTimeout = taskManagerTimeout;

		this.slotManagers = new HashMap<>(16);
		this.taskManagers = new HashMap<>(16);
		this.slotRequests = new HashMap<>(16);
	}


	@Override
	public int getNumberRegisteredSlots() {
		return slotManagers.values()
			.stream()
			.map(SlotManagementWorker::getNumberRegisteredSlots)
			.reduce((left, right) -> left + right)
			.orElse(0);
	}

	@Override
	public int getNumberRegisteredSlotsOf(InstanceID instanceID) {
		return Optional.ofNullable(taskManagers.get(instanceID))
			.flatMap(jobId -> Optional.ofNullable(slotManagers.get(jobId)))
			.map(manager -> manager.getNumberRegisteredSlotsOf(instanceID))
			.orElse(-1);
	}

	@Override
	public int getNumberFreeSlots() {
		return slotManagers.values()
			.stream()
			.map(SlotManagementWorker::getNumberFreeSlots)
			.reduce((left, right) -> left + right)
			.orElse(0);
	}

	@Override
	public int getNumberFreeSlotsOf(InstanceID instanceID) {
		return Optional.ofNullable(taskManagers.get(instanceID))
			.flatMap(jobId -> Optional.ofNullable(slotManagers.get(jobId)))
			.map(manager -> manager.getNumberFreeSlotsOf(instanceID))
			.orElse(-1);
	}

	@Override
	public void start(ResourceManagerId newResourceManagerId,
					  Executor newMainThreadExecutor,
					  ResourceActions newResourceActions) {
		LOG.info("Starting slot manager");
		this.mainThreadExecutor = newMainThreadExecutor;
		this.resourceManagerId = newResourceManagerId;
		this.resourceActions = newResourceActions;

		taskManagerTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				this::checkTaskManagerTimeouts),
			0L,
			taskManagerTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		slotRequestTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				this::checkSlotRequestTimeouts),
			0L,
			slotRequestTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		started = true;
	}

	@Override
	public void suspend() {
		LOG.info("Suspending the SlotManager.");

		// stop the timeout checks for the TaskManagers and the SlotRequests
		if (taskManagerTimeoutCheck != null) {
			taskManagerTimeoutCheck.cancel(false);
			taskManagerTimeoutCheck = null;
		}

		if (slotRequestTimeoutCheck != null) {
			slotRequestTimeoutCheck.cancel(false);
			slotRequestTimeoutCheck = null;
		}

		slotManagers.values()
			.stream()
			.forEach(SlotManagementWorker::suspend);

		this.mainThreadExecutor = null;
		this.resourceActions = null;
		this.resourceManagerId = null;
		slotManagers.clear();
		taskManagers.clear();
		slotRequests.clear();
		started = false;
	}

	@Override
	public boolean registerSlotRequest(SlotRequest slotRequest) throws SlotManagerException {
		checkInit();
		JobID jobID = slotRequest.getJobId();
		SlotManagementWorker slotManager;

		if (!slotManagers.containsKey(jobID)) {
			slotManager = createAndStartSlotManager(jobID);
		} else {
			slotManager = slotManagers.get(jobID);
		}

		slotRequests.put(slotRequest.getAllocationId(), jobID);
		return slotManager.registerSlotRequest(slotRequest);
	}

	@Override
	public boolean unregisterSlotRequest(AllocationID allocationId) {
		checkInit();
		try {
			return Optional.ofNullable(slotRequests.get(allocationId))
				.flatMap(jobID -> Optional.ofNullable(slotManagers.get(jobID)))
				.map(s -> s.unregisterSlotRequest(allocationId))
				.orElse(false);
		} finally {
			slotRequests.remove(allocationId);
		}
	}

	@Override
	public void registerTaskManager(TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		checkInit();

		// If any slot is assigned to a job, then call register of that job
		JobID jobID = null;
		for(SlotStatus status : initialSlotReport) {
			if (status.getJobID() != null) {
				jobID = status.getJobID();
				break;
			}
		}

		if (jobID != null) {
			SlotManagementWorker slotManager;
			if (slotManagers.containsKey(jobID)) {
				slotManager = slotManagers.get(jobID);
			} else {
				slotManager = createAndStartSlotManager(jobID);
			}
			slotManager.registerTaskManager(taskExecutorConnection, initialSlotReport);
			return;
		}

		// If no slot manager found, we assgin this task manager to the first
		for(Map.Entry<JobID, SlotManagementWorker> entry: slotManagers.entrySet()) {
			if (entry.getValue().hasPendingRequests()) {
				LOG.info("Task manager {} assigned to job {}.",
					taskExecutorConnection.getResourceID(), entry.getKey());
				taskManagers.put(taskExecutorConnection.getInstanceID(), entry.getKey());
				entry.getValue().registerTaskManager(taskExecutorConnection, initialSlotReport);
				break;
			}
		}
	}

	@Override
	public boolean unregisterTaskManager(InstanceID instanceId) {
		try {
			return Optional.ofNullable(taskManagers.get(instanceId))
				.flatMap(jobID -> Optional.ofNullable(slotManagers.get(jobID)))
				.map(s -> s.unregisterTaskManager(instanceId))
				.orElse(false);
		} finally {
			taskManagers.remove(instanceId);
		}
	}

	@Override
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		return false;
	}

	@Override
	public void freeSlot(SlotID slotId, AllocationID allocationId) {

	}

	private void checkTaskManagerTimeouts() {
	}

	private void checkSlotRequestTimeouts() {

	}

	private SlotManagementWorker createAndStartSlotManager(JobID jobID) {
		SlotManagementWorker slotManager = new SlotManagementWorker(
			taskManagerTimeout,
			taskManagerRequestTimeout,
			slotRequestTimeout);
		slotManagers.put(jobID, slotManager);
		slotManager.start(resourceManagerId, mainThreadExecutor, resourceActions);

		return slotManager;
	}

	private void checkInit() {
		Preconditions.checkState(started, "Slot manager is not started.");
	}
}
