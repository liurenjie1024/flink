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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * The slot manager is responsible for maintaining a view on all registered task manager slots,
 * their allocation and all pending slot requests. Whenever a new slot is registered or and
 * allocated slot is freed, then it tries to fulfill another pending slot request. Whenever there
 * are not enough slots available the slot manager will notify the resource manager about it via
 * {@link ResourceActions#allocateResource(ResourceProfile)}.
 *
 * <p>In order to free resources and avoid resource leaks, idling task managers (task managers whose
 * slots are currently not used) and pending slot requests time out triggering their release and
 * failure, respectively.
 */
public class SlotManager implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(SlotManager.class);

	private final SlotManagerStrategy strategy;

	public SlotManager(
		ScheduledExecutor scheduledExecutor,
		Time taskManagerRequestTimeout,
		Time slotRequestTimeout,
		Time taskManagerTimeout) {

		this.strategy = new SimpleSlotManagerStrategy(
			scheduledExecutor,
			taskManagerRequestTimeout,
			slotRequestTimeout,
			taskManagerTimeout);
	}



	public int getNumberRegisteredSlots() {
		return strategy.getNumberRegisteredSlots();
	}

	public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
		return strategy.getNumberRegisteredSlotsOf(instanceId);
	}

	public int getNumberFreeSlots() {
		return strategy.getNumberFreeSlots();
	}

	public int getNumberFreeSlotsOf(InstanceID instanceId) {
		return strategy.getNumberRegisteredSlotsOf(instanceId);
	}

	// ---------------------------------------------------------------------------------------------
	// Component lifecycle methods
	// ---------------------------------------------------------------------------------------------

	/**
	 * Starts the slot manager with the given leader id and resource manager actions.
	 *
	 * @param newResourceManagerId to use for communication with the task managers
	 * @param newMainThreadExecutor to use to run code in the ResourceManager's main thread
	 * @param newResourceActions to use for resource (de-)allocations
	 */
	public void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions) {
		strategy.start(newResourceManagerId, newMainThreadExecutor, newResourceActions);
	}

	/**
	 * Suspends the component. This clears the internal state of the slot manager.
	 */
	public void suspend() {
		strategy.suspend();
	}

	/**
	 * Closes the slot manager.
	 *
	 * @throws Exception if the close operation fails
	 */
	@Override
	public void close() throws Exception {
		LOG.info("Closing the SlotManager.");

		suspend();
	}

	// ---------------------------------------------------------------------------------------------
	// Public API
	// ---------------------------------------------------------------------------------------------

	/**
	 * Requests a slot with the respective resource profile.
	 *
	 * @param slotRequest specifying the requested slot specs
	 * @return true if the slot request was registered; false if the request is a duplicate
	 * @throws SlotManagerException if the slot request failed (e.g. not enough resources left)
	 */
	public boolean registerSlotRequest(SlotRequest slotRequest) throws SlotManagerException {
		return strategy.registerSlotRequest(slotRequest);
	}

	/**
	 * Cancels and removes a pending slot request with the given allocation id. If there is no such
	 * pending request, then nothing is done.
	 *
	 * @param allocationId identifying the pending slot request
	 * @return True if a pending slot request was found; otherwise false
	 */
	public boolean unregisterSlotRequest(AllocationID allocationId) {
		return strategy.unregisterSlotRequest(allocationId);
	}

	/**
	 * Registers a new task manager at the slot manager. This will make the task managers slots
	 * known and, thus, available for allocation.
	 *
	 * @param taskExecutorConnection for the new task manager
	 * @param initialSlotReport for the new task manager
	 */
	public void registerTaskManager(final TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		strategy.registerTaskManager(taskExecutorConnection, initialSlotReport);
	}

	/**
	 * Unregisters the task manager identified by the given instance id and its associated slots
	 * from the slot manager.
	 *
	 * @param instanceId identifying the task manager to unregister
	 * @return True if there existed a registered task manager with the given instance id
	 */
	public boolean unregisterTaskManager(InstanceID instanceId) {
		return strategy.unregisterTaskManager(instanceId);
	}

	/**
	 * Reports the current slot allocations for a task manager identified by the given instance id.
	 *
	 * @param instanceId identifying the task manager for which to report the slot status
	 * @param slotReport containing the status for all of its slots
	 * @return true if the slot status has been updated successfully, otherwise false
	 */
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		return strategy.reportSlotStatus(instanceId, slotReport);
	}

	/**
	 * Free the given slot from the given allocation. If the slot is still allocated by the given
	 * allocation id, then the slot will be marked as free and will be subject to new slot requests.
	 *
	 * @param slotId identifying the slot to free
	 * @param allocationId with which the slot is presumably allocated
	 */
	public void freeSlot(SlotID slotId, AllocationID allocationId) {
		strategy.freeSlot(slotId, allocationId);
	}
}
