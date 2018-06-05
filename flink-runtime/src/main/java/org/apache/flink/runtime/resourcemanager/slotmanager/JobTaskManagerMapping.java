package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Objects;
import java.util.Set;

public class JobTaskManagerMapping {
	private final HashMap<InstanceID, JobID> taskMapangerOwner;
	private final HashMap<JobID, Set<InstanceID>> jobOccupation;

	public JobTaskManagerMapping() {
		this.taskMapangerOwner = new HashMap<>(16);
		this.jobOccupation = new HashMap<>(16);
	}

	public boolean canAllocate(JobID jobID, InstanceID instanceID) {
		Preconditions.checkNotNull(jobID);
		Preconditions.checkNotNull(instanceID);

		return Objects.equals(jobID, taskMapangerOwner.get(instanceID));
	}
}
