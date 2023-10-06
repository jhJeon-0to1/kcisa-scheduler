package scheduler.kcisa.job.collection.mobile;

import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import scheduler.kcisa.model.SchedulerStatus;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.utils.Utils;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

@Component
public class MobileCtgryUseQyInfo extends QuartzJobBean {
    SchedulerLogService schedulerLogService;
    DataSource dataSource;
    String tableName = "COLCT_MOBILE_CTGRY_USE_QY_INFO";
    String url = "https://api.dmp.igaworks.com/v1/insight/category-usage";
    WebClient webClient = WebClient.builder().baseUrl(url).build();
    Connection connection;
    @Value("${mobile.api.key}")
    String apiKey;


    public MobileCtgryUseQyInfo(SchedulerLogService schedulerLogService, DataSource dataSource) throws SQLException {
        this.schedulerLogService = schedulerLogService;
        this.dataSource = dataSource;

        connection = dataSource.getConnection();
    }

    @Override
    protected void executeInternal(@NotNull JobExecutionContext context) throws JobExecutionException {
        int count = 0;
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        LocalDate yesterday = LocalDate.now().minusDays(2);
        String date = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        try {
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String insertQuery = "INSERT INTO analysis_etl.COLCT_MOBILE_CTGRY_USE_QY_INFO (COLCT_ID,BASE_DE,BASE_YEAR,BASE_MT,BASE_DAY,UPPER_CTGRY_NM,LWPRT_CTGRY_NM,ALL_EMPR_CO,AOS_EMPR_CO,IOS_EMPR_CO,ALL_USE_TIME,AOS_USE_TIME,IOS_USE_TIME,COLCT_DE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, DATE_FORMAT(NOW(), '%Y%m%d')) ON DUPLICATE KEY UPDATE ALL_EMPR_CO = VALUES(ALL_EMPR_CO), AOS_EMPR_CO = VALUES(AOS_EMPR_CO), IOS_EMPR_CO = VALUES(IOS_EMPR_CO), ALL_USE_TIME = VALUES(ALL_USE_TIME), AOS_USE_TIME = VALUES(AOS_USE_TIME), IOS_USE_TIME = VALUES(IOS_USE_TIME), UPDT_DE = DATE_FORMAT(NOW(), '%Y%m%d')";

            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

            JsonNode response = webClient.get()
                    .uri(uriBuilder -> uriBuilder.queryParam("startDate", date).queryParam("endDate", date).build())
                    .header("Authorization", "Bearer " + apiKey).retrieve().bodyToMono(JsonNode.class).block();

            if (response == null) {
                throw new Exception("jsonResponse is null");
            }
            JsonNode dataList = response.get("data");

            for (JsonNode data : dataList) {
                String dateStr = data.get("date").asText().replace("-", "");
                if (!dateStr.equals(date)) {
                    throw new Exception("date is not equal");
                }
                String categoryMain = data.get("categoryMain").asText();
                String categorySub = data.get("categorySub").asText();
                BigDecimal userTotal = new BigDecimal(data.get("userTotal").asText());
                BigDecimal userAos = new BigDecimal(data.get("userAos").asText());
                BigDecimal userIos = new BigDecimal(data.get("userIos").asText());
                BigDecimal timeTotal = new BigDecimal(data.get("timeTotal").asText());
                BigDecimal timeAos = new BigDecimal(data.get("timeAos").asText());
                BigDecimal timeIos = new BigDecimal(data.get("timeIos").asText());

                preparedStatement.setString(1, dateStr + categoryMain + categorySub);
                preparedStatement.setString(2, dateStr);
                preparedStatement.setString(3, dateStr.substring(0, 4));
                preparedStatement.setString(4, dateStr.substring(4, 6));
                preparedStatement.setString(5, dateStr.substring(6, 8));
                preparedStatement.setString(6, categoryMain);
                preparedStatement.setString(7, categorySub);
                preparedStatement.setBigDecimal(8, userTotal);
                preparedStatement.setBigDecimal(9, userAos);
                preparedStatement.setBigDecimal(10, userIos);
                preparedStatement.setBigDecimal(11, timeTotal);
                preparedStatement.setBigDecimal(12, timeAos);
                preparedStatement.setBigDecimal(13, timeIos);

                preparedStatement.addBatch();
                count++;
            }

            preparedStatement.executeBatch();

            Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
            if (!updt_count.isPresent()) {
                throw new Exception("updt_count is empty");
            }
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count,
                    count - updt_count.get(), updt_count.get()));

        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService
                    .create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
            throw new JobExecutionException(e);
        }
    }
}
