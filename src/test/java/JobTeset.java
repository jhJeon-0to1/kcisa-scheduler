import org.quartz.*;

import java.util.Date;

public class JobTeset {
    public static void main(String[] args) {
        JobExecutionContext context = new JobExecutionContext() {
            @Override
            public Scheduler getScheduler() {
                return null;
            }

            @Override
            public Trigger getTrigger() {
                return null;
            }

            @Override
            public Calendar getCalendar() {
                return null;
            }

            @Override
            public boolean isRecovering() {
                return false;
            }

            @Override
            public TriggerKey getRecoveringTriggerKey() throws IllegalStateException {
                return null;
            }

            @Override
            public int getRefireCount() {
                return 0;
            }

            @Override
            public JobDataMap getMergedJobDataMap() {
                return null;
            }

            @Override
            public JobDetail getJobDetail() {
                return null;
            }

            @Override
            public Job getJobInstance() {
                return null;
            }

            @Override
            public Date getFireTime() {
                return null;
            }

            @Override
            public Date getScheduledFireTime() {
                return null;
            }

            @Override
            public Date getPreviousFireTime() {
                return null;
            }

            @Override
            public Date getNextFireTime() {
                return null;
            }

            @Override
            public String getFireInstanceId() {
                return null;
            }

            @Override
            public Object getResult() {
                return null;
            }

            @Override
            public void setResult(Object result) {

            }

            @Override
            public long getJobRunTime() {
                return 0;
            }

            @Override
            public void put(Object key, Object value) {

            }

            @Override
            public Object get(Object key) {
                return null;
            }
        };
        System.out.println(context.getJobDetail().getKey().getGroup());

    }
}
