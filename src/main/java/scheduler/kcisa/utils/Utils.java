package scheduler.kcisa.utils;

import org.quartz.JobExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

@Component
public class Utils {
    static MartSchedulerLogService martSchedulerLogService;
    static DataSource dataSource;

    @Autowired
    public Utils(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) throws Exception {
        Utils.martSchedulerLogService = martSchedulerLogService;
        Utils.dataSource = dataSource;
    }

    public static Optional<Integer> getUpdtCount(String tableName) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();

            List<String> columns = Arrays.asList("UPDT_DE", "UPDT_YM", "UPDT_DT");

            String columnToUse = null;
            for (String column : columns) {
                ResultSet resultSet = metaData.getColumns(null, null, tableName, column);
                if (resultSet.next()) {
                    columnToUse = column;
                    break;
                }
            }

            if (columnToUse == null) {
                throw new Exception("수정 날짜를 확인할 수 없습니다.");
            } else {
                String query = "SELECT COUNT(*) FROM analysis_etl." + tableName
                        + " WHERE DATE_FORMAT(" + columnToUse + ", '%Y%m%d') = DATE_FORMAT(NOW(), '%Y%m%d')";
                PreparedStatement preparedStatement = connection.prepareStatement(query);

                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    return Optional.of(resultSet.getInt(1));
                } else {
                    return Optional.empty();
                }
            }
        }
    }

    public static Boolean checkAlreadyExist(String tableName, String date, JobExecutionContext context)
            throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            System.out.println(tableName + " 테이블의 " + date + " 의 데이터를 분석합니다.");

            boolean isMonth = date.length() == 6;
            boolean isYear = date.length() == 4;
            String dateQuery = isYear ? "BASE_YEAR" : isMonth ? "BASE_YM" : "BASE_DE";

            String query = "SELECT COUNT(*) AS count FROM analysis_etl." + tableName.toLowerCase() + " WHERE " + dateQuery + "= ?";
            PreparedStatement countPstmt = connection.prepareStatement(query);
            countPstmt.setString(1, date);

            ResultSet countRs = countPstmt.executeQuery();
            int count = 0;
            if (countRs.next()) {
                count = countRs.getInt("count");
            }
            countPstmt.close();

            if (count > 0) {
                System.out.println(tableName + " 테이블의 " + date + " 이미 진행된 데이터입니다.");

                return true;
            } else {
                String groupName = context.getJobDetail().getKey().getGroup();
                String jobName = context.getJobDetail().getKey().getName();

                martSchedulerLogService
                        .create(new MartSchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("분석된 데이터를 확인하는 중 오류가 발생했습니다.");
        }
    }

    public static String getSQLString(String path) throws Exception {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8))) {
            StringBuilder stringBuilder = new StringBuilder();
            String line;

            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line).append("\n");
            }

            return stringBuilder.toString();
        }
    }
}
