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
                Arrays.asList("mobile_이용량", "sports_시도별관중", "popltn_info", "sports_일별경기", "pblprfr_viewing_info", "kopis_공연시설", "kopis_공연시설상세", "kobis_지역별일별", "kobis_일별매출액", "kobis_movie", "kopis_공연시설통합")
        );

        if (!allowedTables.contains(tableName)) {
            throw new Exception("tableName is not allowed");
        }

        try {
            String query = "SELECT COUNT(*) FROM kcisa." + tableName + " WHERE DATE_FORMAT(updt_dt, '%Y%m%d') = DATE_FORMAT(NOW(), '%Y%m%d')";
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
}
