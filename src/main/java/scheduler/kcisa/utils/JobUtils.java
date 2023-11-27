package scheduler.kcisa.utils;

import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.model.flag.analysis.DailyAnalysisFlag;
import scheduler.kcisa.model.flag.analysis.MonthlyAnalysisFlag;
import scheduler.kcisa.model.flag.analysis.YearlyAnalysisFlag;
import scheduler.kcisa.model.flag.collection.DailyCollectionFlag;
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;
import scheduler.kcisa.model.flag.collection.YearlyCollectionFlag;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.LogService;
import scheduler.kcisa.service.MartSchedulerLogService;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.service.flag.analysis.DailyAnalysisFlagService;
import scheduler.kcisa.service.flag.analysis.MonthlyAnalysisFlagService;
import scheduler.kcisa.service.flag.analysis.YearlyAnalysisFlagService;
import scheduler.kcisa.service.flag.collection.DailyCollectionFlagService;
import scheduler.kcisa.service.flag.collection.MonthlyCollectionFlagService;
import scheduler.kcisa.service.flag.collection.YearlyCollectionFlagService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

@Component
public class JobUtils {
    static MartSchedulerLogService martSchedulerLogService;
    static SchedulerLogService schedulerLogService;
    static DataSource dataSource;
    static DailyCollectionFlagService dailyCollectionFlagService;
    static MonthlyCollectionFlagService monthlyCollectionFlagService;
    static YearlyCollectionFlagService yearlyCollectionFlagService;
    static DailyAnalysisFlagService dailyAnalysisFlagService;
    static MonthlyAnalysisFlagService monthlyAnalysisFlagService;
    static YearlyAnalysisFlagService yearlyAnalysisFlagService;


    @Autowired
    public JobUtils(MartSchedulerLogService martSchedulerLogService, SchedulerLogService schedulerLogService, DataSource dataSource, DailyCollectionFlagService dailyCollectionFlagService, MonthlyCollectionFlagService monthlyCollectionFlagService, YearlyCollectionFlagService yearlyCollectionFlagService, DailyAnalysisFlagService dailyAnalysisFlagService, YearlyAnalysisFlagService yearlyAnalysisFlagService, MonthlyAnalysisFlagService monthlyAnalysisFlagService) throws SQLException {
        JobUtils.martSchedulerLogService = martSchedulerLogService;
        JobUtils.schedulerLogService = schedulerLogService;
        JobUtils.dataSource = dataSource;
        JobUtils.dailyCollectionFlagService = dailyCollectionFlagService;
        JobUtils.monthlyCollectionFlagService = monthlyCollectionFlagService;
        JobUtils.yearlyCollectionFlagService = yearlyCollectionFlagService;
        JobUtils.dailyAnalysisFlagService = dailyAnalysisFlagService;
        JobUtils.monthlyAnalysisFlagService = monthlyAnalysisFlagService;
        JobUtils.yearlyAnalysisFlagService = yearlyAnalysisFlagService;
    }

    public static void executeJob(JobExecutionContext context, String tableName, ThrowableConsumer<JobData, Exception> jobTask) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        try (Connection connection = dataSource.getConnection()) {
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));
            jobTask.accept(new JobData(groupName, jobName, tableName, connection, "colct"));
        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }

    public static void executeAnalysisJob(JobExecutionContext context, String tableName, List<String> checkList, String flagDate, ScheduleInterval interval, ThrowableConsumer<JobData, Exception> jobTask) {
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        List<String> analysisTableList = Arrays.asList(tableName);

        try (Connection connection = dataSource.getConnection()) {
            boolean isExist = checkAnalysisFlag(analysisTableList, flagDate, interval);
            if (isExist) {
                System.out.println(tableName + " 분석은 이미 완료되었습니다.");
                return;
            }

            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String emptyTableName = checkCollectFlag(checkList, flagDate, interval);
            if (emptyTableName != null) {
                martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, emptyTableName + " 분석을 위한 수집이 완료되지 않았습니다."));
                return;
            }

            jobTask.accept(new JobData(groupName, jobName, tableName, connection, "analysis"));
        } catch (Exception e) {
            e.printStackTrace();
            martSchedulerLogService.create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }


    public static String checkCollectFlag(List<String> tableList, String flagDate, ScheduleInterval interval) {
        String result = null;

        if (tableList.contains("ctprvn_accto_popltn_info")) {
            tableList.remove("ctprvn_accto_popltn_info");
            boolean checked = checkPopltn();
            if (!checked) {
//                ctprvn_accto_popltn_info flag가 없으면 false 따라서 ctprvn_accto_popltn_info가 없는 테이블 명이 된다.
                result = "ctprvn_accto_popltn_info";
            }
        }

        if (result == null) {
            for (String tableName : tableList) {
                switch (interval) {
                    case DAILY:
                        DailyCollectionFlag flag = dailyCollectionFlagService.findByDateAndTableName(flagDate, tableName);

                        if (flag == null) {
                            result = tableName;
                        }
                        break;
                    case MONTHLY:
                        MonthlyCollectionFlag monthlyFlag = monthlyCollectionFlagService.findByDateAndTableName(flagDate, tableName);

                        if (monthlyFlag == null) {
                            result = tableName;
                        }
                        break;
                    case YEARLY:
                        YearlyCollectionFlag yearlyFlag = yearlyCollectionFlagService.findByDateAndTableName(flagDate, tableName);

                        if (yearlyFlag == null) {
                            result = tableName;
                        }
                        break;
                }
                if (result != null) {
                    break;
                }
            }
        }

        return result;
    }

    public static boolean checkAnalysisFlag(List<String> tableList, String flagDate, ScheduleInterval interval) {
        boolean result = false;

        for (String tableName : tableList) {
            switch (interval) {
                case DAILY:
                    DailyAnalysisFlag flag = dailyAnalysisFlagService.findByDateAndTableName(flagDate, tableName);

                    result = flag != null;
                    break;
                case MONTHLY:
                    MonthlyAnalysisFlag monthlyFlag = monthlyAnalysisFlagService.findByDateAndTableName(flagDate, tableName);

                    result = monthlyFlag != null;
                    break;
                case YEARLY:
                    YearlyAnalysisFlag yearlyFlag = yearlyAnalysisFlagService.findByDateAndTableName(flagDate, tableName);

                    result = yearlyFlag != null;
                    break;
            }
        }

        return result;
    }

    //    ctprvn_accto_popltn_info flag가 있는지 확인
//    있으면 true, 없으면 false
    public static boolean checkPopltn() {
        String flagDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        MonthlyCollectionFlag flag = monthlyCollectionFlagService.findByDateAndTableName(flagDate, "ctprvn_accto_popltn_info");
//        ctprvn_accto_popltn_info flag가 없으면 false
        return flag != null;
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

