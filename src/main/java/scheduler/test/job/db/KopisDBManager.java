package scheduler.test.job.db;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class KopisDBManager {
    private final DataSource dataSource;

    public KopisDBManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Map<String, Object> insertRow(List<JsonNode> rows) {
        try {
            Connection conn = dataSource.getConnection();

            String query = "INSERT INTO pblprfr_viewing_info (BASE_DE, BASE_YEAR, BASE_MT, BASE_DAY, CTPRVN_CD, CTPRVN_NM, GENRE_CD, GENRE_NM, PBLPRFR_RASNG_CUTIN_CO, PBLPRFR_RASNG_CUTIN_OCCU_RT, PBLPRFR_CO, PBLPRFR_OCCU_RT, PBLPRFR_STGNG_CO, PBLPRFR_STGNG_OCCU_RT, PBLPRFR_SALES_PRICE, PBLPRFR_SALES_PRICE_RT, PBLPRFR_VIEWNG_NMPR_CO, PBLPRFR_VIEWNG_NMPR_RT) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            PreparedStatement pstmt = conn.prepareStatement(query);

            for (JsonNode row : rows) {
                pstmt.setString(1, row.get("BASE_DE").asText());
                pstmt.setString(2, row.get("BASE_YEAR").asText());
                pstmt.setString(3, row.get("BASE_MT").asText());
                pstmt.setString(4, row.get("BASE_DAY").asText());
                pstmt.setString(5, row.get("CTPRVN_CD").asText());
                pstmt.setString(6, row.get("CTPRVN_NM").asText());
                pstmt.setString(7, row.get("GENRE_CD").asText());
                pstmt.setString(8, row.get("GENRE_NM").asText());
                pstmt.setBigDecimal(9, new BigDecimal(row.get("PBLPRFR_RASNG_CUTIN_CO").asText()));
                pstmt.setBigDecimal(10, new BigDecimal(row.get("PBLPRFR_RASNG_CUTIN_OCCU_RT").asText()));
                pstmt.setBigDecimal(11, new BigDecimal(row.get("PBLPRFR_CO").asText()));
                pstmt.setBigDecimal(12, new BigDecimal(row.get("PBLPRFR_OCCU_RT").asText()));
                pstmt.setBigDecimal(13, new BigDecimal(row.get("PBLPRFR_STGNG_CO").asText()));
                pstmt.setBigDecimal(14, new BigDecimal(row.get("PBLPRFR_STGNG_OCCU_RT").asText()));
                pstmt.setBigDecimal(15, new BigDecimal(row.get("PBLPRFR_SALES_PRICE").asText()));
                pstmt.setBigDecimal(16, new BigDecimal(row.get("PBLPRFR_SALES_PRICE_RT").asText()));
                pstmt.setBigDecimal(17, new BigDecimal(row.get("PBLPRFR_VIEWNG_NMPR_CO").asText()));
                pstmt.setBigDecimal(18, new BigDecimal(row.get("PBLPRFR_VIEWNG_NMPR_RT").asText()));

                pstmt.addBatch();
            }

            System.out.println(pstmt.toString());

            pstmt.executeBatch();

            pstmt.close();
            conn.close();

            Map<String, Object> map = new HashMap<>();
            map.put("isSuccess", true);
            return map;
        } catch (Exception e) {
            e.printStackTrace();
            Map<String, Object> map = new HashMap<>();
            map.put("isSuccess", false);
            map.put("message", e.getMessage());
            return map;
        }
    }
}
