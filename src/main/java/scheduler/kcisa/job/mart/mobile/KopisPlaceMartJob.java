package scheduler.kcisa.job.mart.mobile;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import javax.sql.DataSource;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.utils.Utils;

public class KopisPlaceMartJob extends QuartzJobBean {
    DataSource dataSource;
    MartSchedulerLogService martSchedulerLogService;
    Connection connection;

    public KopisPlaceMartJob(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) {
        this.dataSource = dataSource;
        this.martSchedulerLogService = martSchedulerLogService;
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        String tableName = "PBLPRFR_FCLTY_CRSTAT";
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        LocalDate now = LocalDate.now();
        String date = now.format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String ym = now.format(DateTimeFormatter.ofPattern("yyyyMM"));

        try {
            connection = dataSource.getConnection();

            Boolean isExist = Utils.checkAlreadyExist(tableName, ym);

            if (isExist) {
                System.out.println("이미 분석된 데이터입니다.");

                return;
            }

            martSchedulerLogService
                    .create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String query = "INSERT INTO PBLPRFR_FCLTY_CRSTAT (BASE_YM, BASE_YEAR, BASE_MT, CTPRVN_CD, CTPRVN_NM, LRGE_THEAT_CO, MIDDL_THEAT_CO, SMALL_THEAT_CO) SELECT BASE_YM, substr(BASE_YM, 1, 4) as BASE_YEAR, SUBSTR(BASE_YM, 5, 2) as BASE_MT, CTPRVN_CD, MAX(CTPRVN_NM) as CTPRVN_NM, SUM(if(FCLTY_SEAT_CO >= 1000, 1,0)) as LRGE_THEAT_CO, SUM(if(FCLTY_SEAT_CO > 300 and FCLTY_SEAT_CO < 1000, 1, 0)) as MIDDL_THEAT_CO, SUM(if(FCLTY_SEAT_CO <= 300, 1, 0)) as SMALL_THEAT_CO from (select ? as BASE_YM, A.PBLPRFR_FCLTY_ID, A.PBLPRFR_FCLTY_NM, A.CTPRVN_CD, A.CTPRVN_NM, A.OPNNG_YEAR, B.FCLTY_SEAT_CO from colct_pblprfr_fclty_info as A join colct_pblprfr_fclty_detail_info as B on A.PBLPRFR_FCLTY_ID  = B.PBLPRFR_FCLTY_ID and A.COLCT_YM = B.COLCT_YM where A.COLCT_YM = ?) as data group by BASE_YM, CTPRVN_CD";

            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setString(1, ym);
            pstmt.setString(2, date);

            int count = pstmt.executeUpdate();

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS,
                    count));
        } catch (Exception e) {
            martSchedulerLogService.create(
                    new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }
}
