package scheduler.kcisa.utils;

import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.LogService;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.service.SchedulerLogService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

@Component
public class JobUtils {
    static MartSchedulerLogService martSchedulerLogService;
    static SchedulerLogService schedulerLogService;
    static DataSource dataSource;


    @Autowired
    public JobUtils(MartSchedulerLogService martSchedulerLogService, SchedulerLogService schedulerLogService, DataSource dataSource) throws SQLException {
        JobUtils.martSchedulerLogService = martSchedulerLogService;
        JobUtils.schedulerLogService = schedulerLogService;
        JobUtils.dataSource = dataSource;
    }

    public static void executeJob(JobExecutionContext context, String tableName, ThrowableConsumer<JobData, Exception> jobTask) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        Connection conn = null;

        try {
            conn = dataSource.getConnection();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));
            jobTask.accept(new JobData(groupName, jobName, tableName, conn, "colct"));
        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        } finally {
            try {
                if (Objects.nonNull(conn)) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void executeAnalysisJob(JobExecutionContext context, String tableName, String stdDateStr, ThrowableConsumer<JobData, Exception> jobTask) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();
        Connection conn = null;

        try {
            conn = dataSource.getConnection();

            Boolean isExist = Utils.checkAlreadyExist(tableName, stdDateStr, context);

            if (isExist) {
                return;
            }
            jobTask.accept(new JobData(groupName, jobName, tableName, conn, "analysis"));
        } catch (Exception e) {
            e.printStackTrace();
            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        } finally {
            try {
                if (Objects.nonNull(conn)) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @FunctionalInterface
    public interface ThrowableConsumer<T, E extends Throwable> {
        void accept(T t) throws E;
    }

    public static class JobData {
        public String groupName;
        public String jobName;
        public String tableName;
        public Connection conn;
        public LogService logService;


        public JobData(String groupName, String jobName, String tableName, Connection conn, String jobType) {
            this.groupName = groupName;
            this.jobName = jobName;
            this.tableName = tableName;
            this.conn = conn;

            if (Objects.equals(jobType, "analysis")) {
                this.logService = martSchedulerLogService;
            } else {
                this.logService = schedulerLogService;
            }
        }
    }
}
