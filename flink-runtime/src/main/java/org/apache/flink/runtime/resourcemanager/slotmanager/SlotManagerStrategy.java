package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;

/**
 * The implementation of this interface is used to replace SlotManager, so its method are just
 * public method of SlotManager. Currently we still use SlotManager as the facade so that we
 * don't need to change much code before the community agrees with our design.
 * We have two slot manager strategies: job isolated and non job isolation. The implementation
 * without job isolation exists to be compatible with existing code and tests.
 */
interface SlotManagerStrategy {
	int getNumberRegisteredSlots();
	int getNumberRegisteredSlotsOf(InstanceID instanceID);

	int getNumberFreeSlots();
	int getNumberFreeSlotsOf(InstanceID instanceID);

	void start(ResourceManagerId newResourceManagerId,
			   Executor newMainThreadExecutor,
			   ResourceActions newResourceActions);

	void suspend();
	default void close() {
		suspend();
	}

	boolean registerSlotRequest(SlotRequest slotRequest) throws SlotManagerException;
	boolean unregisterSlotRequest(AllocationID allocationId);

	void registerTaskManager(final TaskExecutorConnection taskExecutorConnection,
							 SlotReport initialSlotReport);
	boolean unregisterTaskManager(InstanceID instanceId);

	boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport);
	void freeSlot(SlotID slotId, AllocationID allocationId);
}
