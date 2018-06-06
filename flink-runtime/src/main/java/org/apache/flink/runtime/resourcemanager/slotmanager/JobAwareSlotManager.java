package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;

import java.util.HashMap;

public class JobAwareSlotManager {
	private final HashMap<JobID, SlotManagementWorker> jobSlotManager;
}
