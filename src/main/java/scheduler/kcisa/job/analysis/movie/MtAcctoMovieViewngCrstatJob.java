package scheduler.kcisa.job.analysis.movie;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component
public class MtAcctoMovieViewngCrstatJob extends QuartzJobBean {
    DataSource dataSource;
    MartSchedulerLogService martSchedulerLogService;
    String tableName = "MOVIE_MT_ACCTO_VIEWNG_CRSTAT";
    Connection connection;

    public MtAcctoMovieViewngCrstatJob(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        LocalDate stdDate = LocalDate.now().minusMonths(1);
        String stdDateStr = stdDate.format(DateTimeFormatter.ofPattern("yyyyMM"));
        try {
            connection = dataSource.getConnection();

            Boolean isExist = Utils.checkAlreadyExist(tableName, stdDateStr, context);

            if (isExist) {
                return;
            }

            String query = Utils.getSQLString("src/main/resources/sql/analysis/movie/MtAcctoMovieViewngCrstat.sql");

            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, stdDateStr);
            pstmt.setString(2, stdDateStr);

            int count = pstmt.executeUpdate();

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count));
        } catch (Exception e) {
            e.printStackTrace();
            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}