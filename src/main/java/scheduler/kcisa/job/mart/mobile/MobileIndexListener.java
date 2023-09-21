package scheduler.kcisa.job.mart.mobile;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobListener;

public class MobileIndexListener implements JobListener {
    @Override
    public String getName() {
        return "MobileIndexJob";
    }

    @Override
    public void jobToBeExecuted(JobExecutionContext context) {

    }

    @Override
    public void jobExecutionVetoed(JobExecutionContext context) {

    }

    @Override
    public void jobWasExecuted(JobExecutionContext context, JobExecutionException jobException) {
        if (jobException != null) {
            System.out.println("MobileIndexJob Failed");
        } else {
            System.out.println("MobileIndexJob Success");
        }
    }
}
