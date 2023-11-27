package scheduler.kcisa.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class JobInfo {
    String groupName;
    String jobName;
    SchedulerType schedulerType;
    String cronExpression;
    long repeatInterval;

    public JobInfo(String groupName, String jobName, SchedulerType schedulerType, String cronExpression) {
        this.groupName = groupName;
        this.jobName = jobName;
        this.schedulerType = schedulerType;
        this.cronExpression = cronExpression;
    }

    public JobInfo(String groupName, String jobName, SchedulerType schedulerType, long repeatInterval) {
        this.groupName = groupName;
        this.jobName = jobName;
        this.schedulerType = schedulerType;
        this.repeatInterval = repeatInterval;
    }
}
