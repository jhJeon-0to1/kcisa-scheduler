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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Optional;

@Component
public class Utils {
    static MartSchedulerLogService martSchedulerLogService;
    private static Connection connection;

    @Autowired
    public Utils(DataSource dataSource, MartSchedulerLogService martSchedulerLogService) throws Exception {
        connection = dataSource.getConnection();
        Utils.martSchedulerLogService = martSchedulerLogService;
    }

    public static Optional<Integer> getUpdtCount(String tableName) throws Exception {
        try {
            String query = "SELECT COUNT(*) FROM analysis_etl." + tableName
                    + " WHERE DATE_FORMAT(UPDT_DE, '%Y%m%d') = DATE_FORMAT(NOW(), '%Y%m%d')";
            PreparedStatement preparedStatement = connection.prepareStatement(query);

            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return Optional.of(resultSet.getInt(1));
            } else {
                return Optional.empty();
            }
        } catch (Exception e) {
            try {
                String query = "SELECT COUNT(*) FROM analysis_etl." + tableName
                        + " WHERE UPDT_YM = DATE_FORMAT(NOW(), '%Y%m')";
                PreparedStatement preparedStatement = connection.prepareStatement(query);

                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    return Optional.of(resultSet.getInt(1));
                } else {
                    return Optional.empty();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
                throw new Exception("getUpdtCount error");
            }
        }
    }

    public static Boolean checkAlreadyExist(String tableName, String date, JobExecutionContext context)
            throws Exception {
        try {
            System.out.println(tableName + " 테이블의 " + date + " 의 데이터를 분석합니다.");

            Boolean isMonth = date.length() == 6;
            String dateQuery = isMonth ? "BASE_YM" : "BASE_DE";

            String query = "SELECT COUNT(*) AS count FROM analysis_etl." + tableName + " WHERE " + dateQuery + "= ?";
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
