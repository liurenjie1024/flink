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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;

public class JobAwareSlotManagerStrategy implements SlotManagerStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(JobAwareSlotManagerStrategy.class);

	private final Map<JobID, SlotManagementWorker> slotManagers;
	private final Map<InstanceID, JobID> taskManagers;

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
	public void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions) {
	}

	@Override
	public void suspend() {

	}

	@Override
	public boolean registerSlotRequest(SlotRequest slotRequest) throws SlotManagerException {
		return false;
	}

	@Override
	public boolean unregisterSlotRequest(AllocationID allocationId) {
		return false;
	}

	@Override
	public void registerTaskManager(TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {

	}

	@Override
	public boolean unregisterTaskManager(InstanceID instanceId) {
		return false;
	}

	@Override
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		return false;
	}

	@Override
	public void freeSlot(SlotID slotId, AllocationID allocationId) {

	}
}
