package scheduler.kcisa.job.mart.sports;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import javax.sql.DataSource;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;

import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.utils.Utils;

public class SportsMatchJob extends QuartzJobBean {
    DataSource dataSource;
    MartSchedulerLogService martSchedulerLogService;
    Connection connection;
    String tableName = "SPORTS_MATCH_CRSTAT";

    @Autowired
    public SportsMatchJob(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
    
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        LocalDate now = LocalDate.now();
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        try {
            connection = dataSource.getConnection();

            Boolean isExist = Utils.checkAlreadyExist(tableName, date, context);

            if (isExist) {
                System.out.println(date + " " + tableName + " is already exist");

                return;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }
}
