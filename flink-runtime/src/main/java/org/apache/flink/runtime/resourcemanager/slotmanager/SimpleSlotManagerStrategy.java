package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SimpleSlotManagerStrategy implements SlotManagerStrategy {
	private static final Logger LOG = LoggerFactory.getLogger(SimpleSlotManagerStrategy.class);

	private final SlotManagementWorker worker;
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

	/** True iff the component has been started. */
	private boolean started;

	SimpleSlotManagerStrategy(ScheduledExecutor scheduledExecutor,
							  Time taskManagerRequestTimeout,
							  Time slotRequestTimeout,
							  Time taskManagerTimeout) {
		this.taskManagerRequestTimeout = Preconditions.checkNotNull(taskManagerRequestTimeout);
		this.slotRequestTimeout = Preconditions.checkNotNull(slotRequestTimeout);
		this.taskManagerTimeout = Preconditions.checkNotNull(taskManagerTimeout);
		this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor);

		this.worker = new SlotManagementWorker(
			taskManagerTimeout,
			taskManagerRequestTimeout,
			slotRequestTimeout);
	}

	@Override
	public int getNumberRegisteredSlots() {
		return worker.getNumberRegisteredSlots();
	}

	@Override
	public int getNumberRegisteredSlotsOf(InstanceID instanceID) {
		return worker.getNumberRegisteredSlotsOf(instanceID);
	}

	@Override
	public int getNumberFreeSlots() {
		return worker.getNumberFreeSlots();
	}

	@Override
	public int getNumberFreeSlotsOf(InstanceID instanceID) {
		return worker.getNumberFreeSlotsOf(instanceID);
	}

	@Override
	public void start(ResourceManagerId newResourceManagerId, Executor newMainThreadExecutor, ResourceActions newResourceActions) {
		LOG.info("Starting the SlotManager.");


		taskManagerTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				() -> checkTaskManagerTimeouts()),
			0L,
			taskManagerTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		slotRequestTimeoutCheck = scheduledExecutor.scheduleWithFixedDelay(
			() -> mainThreadExecutor.execute(
				() -> checkSlotRequestTimeouts()),
			0L,
			slotRequestTimeout.toMilliseconds(),
			TimeUnit.MILLISECONDS);

		worker.start(newResourceManagerId, newMainThreadExecutor, newResourceActions);
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

		worker.suspend();
		started = false;
	}

	@Override
	public boolean registerSlotRequest(SlotRequest slotRequest) throws SlotManagerException {
		return worker.registerSlotRequest(slotRequest);
	}

	@Override
	public boolean unregisterSlotRequest(AllocationID allocationId) {
		return worker.unregisterSlotRequest(allocationId);
	}

	@Override
	public void registerTaskManager(TaskExecutorConnection taskExecutorConnection, SlotReport initialSlotReport) {
		worker.registerTaskManager(taskExecutorConnection, initialSlotReport);
	}

	@Override
	public boolean unregisterTaskManager(InstanceID instanceId) {
		return worker.unregisterTaskManager(instanceId);
	}

	@Override
	public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
		return worker.reportSlotStatus(instanceId, slotReport);
	}

	@Override
	public void freeSlot(SlotID slotId, AllocationID allocationId) {
		worker.freeSlot(slotId, allocationId);
	}

	private void checkTaskManagerTimeouts() {
		worker.checkTaskManagerTimeouts();
	}

	private void checkSlotRequestTimeouts() {
		worker.checkSlotRequestTimeouts();
	}
}
