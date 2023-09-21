package scheduler.kcisa.job.collection.mobile;

import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
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
public class MobileIndexJob extends QuartzJobBean {
    SchedulerLogService schedulerLogService;
    DataSource dataSource;
    String tableName = "mobile_이용량";
    String url = "https://api.dmp.igaworks.com/v1/insight/category-usage";
    WebClient webClient = WebClient.builder().baseUrl(url).build();
    Connection connection;
    @Value("${mobile.api.key}")
    String apiKey;

    @Autowired
    public MobileIndexJob(SchedulerLogService schedulerLogService, DataSource dataSource) throws SQLException {
        this.schedulerLogService = schedulerLogService;
        this.dataSource = dataSource;

        connection = dataSource.getConnection();
    }

    @Override
    protected void executeInternal(@NotNull JobExecutionContext context) throws JobExecutionException {
        int count = 0;
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        LocalDate yesterday = LocalDate.now().minusDays(1);
        String date = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        try {
            System.out.println("MobileIndexJob Start");
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));

            String insertQuery = "INSERT INTO kcisa.mobile_이용량 (date, categoryMain, categorySub, userTotal, userAos, userIos, timeTotal, timeAos, timeIos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE userTotal = VALUES(userTotal), userAos = VALUES(userAos), userIos = VALUES(userIos), timeTotal = VALUES(timeTotal), timeAos = VALUES(timeAos), timeIos = VALUES(timeIos), updt_dt = CURRENT_TIMESTAMP";
            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

            JsonNode response = webClient.get().uri(uriBuilder -> uriBuilder.queryParam("startDate", date).queryParam("endDate", date).build()).header("Authorization", "Bearer " + apiKey).retrieve().bodyToMono(JsonNode.class).block();

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
                double userTotal = data.get("userTotal").asDouble();
                double userAos = data.get("userAos").asDouble();
                double userIos = data.get("userIos").asDouble();
                double timeTotal = data.get("timeTotal").asDouble();
                double timeAos = data.get("timeAos").asDouble();
                double timeIos = data.get("timeIos").asDouble();

                preparedStatement.setString(1, dateStr);
                preparedStatement.setString(2, categoryMain);
                preparedStatement.setString(3, categorySub);
                preparedStatement.setBigDecimal(4, new BigDecimal(userTotal));
                preparedStatement.setBigDecimal(5, new BigDecimal(userAos));
                preparedStatement.setBigDecimal(6, new BigDecimal(userIos));
                preparedStatement.setBigDecimal(7, new BigDecimal(timeTotal));
                preparedStatement.setBigDecimal(8, new BigDecimal(timeAos));
                preparedStatement.setBigDecimal(9, new BigDecimal(timeIos));

                preparedStatement.addBatch();
                count++;
            }

            preparedStatement.executeBatch();

            Optional<Integer> updt_count = Utils.getUpdtCount(tableName);
            if (!updt_count.isPresent()) {
                throw new Exception("updt_count is empty");
            }
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count, count - updt_count.get(), updt_count.get()));

        } catch (Exception e) {
            e.printStackTrace();
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
            throw new JobExecutionException(e);
        }
    }
}
