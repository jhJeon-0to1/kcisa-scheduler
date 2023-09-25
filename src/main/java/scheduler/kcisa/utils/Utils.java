package scheduler.kcisa.utils;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

@Component
public class Utils {
    private static Connection connection;

    @Autowired
    public Utils(DataSource dataSource) throws Exception {
        connection = dataSource.getConnection();
    }

    public static Optional<Integer> getUpdtCount(String tableName) throws Exception {
        Set<String> allowedTables = new HashSet<>(
                Arrays.asList("COLCT_SPORTS_MATCH_INFO", "COLCT_SPORTS_VIEWNG_INFO", "COLCT_MOBILE_CTGRY_USE_QY_INFO",
                        "CTPRVN_ACCTO_POPLTN_INFO", "COLCT_PBLPRFR_FCLTY_INFO", "COLCT_PBLPRFR_FCLTY_DETAIL_INFO"));

        if (!allowedTables.contains(tableName)) {
            throw new Exception("tableName is not allowed");
        }

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
            e.printStackTrace();
            throw new Exception("getUpdtCount error");
        }
    }

    public static Boolean checkAlreadyExist(String tableName, String date) throws Exception {
        try {
            Boolean isMonth = date.length() == 6;
            String dateQuery = isMonth ? "BASE_YM" : "BASE_DT";

            String query = "SELECT COUNT(*) AS count FROM analysis_etl." + tableName + " WHERE " + dateQuery + "= ?";
            PreparedStatement countPstmt = connection.prepareStatement(query);
            countPstmt.setString(1, date);

            ResultSet countRs = countPstmt.executeQuery();
            int count = 0;
            if (countRs.next()) {
                count = countRs.getInt("count");
            }
            countPstmt.close();

            return count > 0;
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("분석된 데이터를 확인하는 중 오류가 발생했습니다.");
        }
    }
}
