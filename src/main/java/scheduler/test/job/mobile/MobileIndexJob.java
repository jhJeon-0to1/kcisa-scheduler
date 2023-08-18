package scheduler.test.job.mobile;

import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import scheduler.test.model.SchedulerLog;
import scheduler.test.model.SchedulerStatus;
import scheduler.test.service.SchedulerLogService;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

@Component
public class MobileIndexJob extends QuartzJobBean {
    SchedulerLogService schedulerLogService;
    DataSource dataSource;
    String tableName = "mobile_이용량";
    String url = "https://api.dmp.igaworks.com/v1/insight/category-usage";

    @Autowired
    public MobileIndexJob(SchedulerLogService schedulerLogService, DataSource dataSource) {
        this.schedulerLogService = schedulerLogService;
        this.dataSource = dataSource;
    }

    @Override
    protected void executeInternal(@NotNull org.quartz.JobExecutionContext context) {
        int count = 0;
        String groupName = context.getJobDetail().getKey().getGroup();
        String jobName = context.getJobDetail().getKey().getName();

        LocalDate yesterday = LocalDate.now().minusDays(1);
        String date = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        try {
            System.out.println("MobileIndexJob Start");
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.STARTED));
            Connection connection = dataSource.getConnection();
            String insertQuery = "INSERT INTO kcisa.mobile_이용량 (date, categoryMain, categorySub, userTotal, userAos, userIos, timeTotal, timeAos, timeIos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);

            WebClient webClient = WebClient.builder().baseUrl(url).build();

            Mono<JsonNode> response = webClient.get().uri(uriBuilder -> uriBuilder.queryParam("startDate", date).queryParam("endDate", date).build()).header("Authorization", "Bearer nsR8FLfYGat2AnndzqgzHC+NdUU4UPUvNO8YtLNlDwGm0L/WjPkMcAf4SM8MGs6daL7KMnkPKGl2eBLh7jAWiw==").retrieve().bodyToMono(JsonNode.class);

            JsonNode jsonResponse = response.block();
            JsonNode dataList = jsonResponse.get("data");

            for (JsonNode data : dataList) {
                String categoryMain = data.get("categoryMain").asText();
                String categorySub = data.get("categorySub").asText();
                double userTotal = data.get("userTotal").asDouble();
                double userAos = data.get("userAos").asDouble();
                double userIos = data.get("userIos").asDouble();
                double timeTotal = data.get("timeTotal").asDouble();
                double timeAos = data.get("timeAos").asDouble();
                double timeIos = data.get("timeIos").asDouble();

                preparedStatement.setString(1, date);
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
            System.out.println("MobileIndexJob End");
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.SUCCESS, count));
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("MobileIndexJob Error");
            schedulerLogService.create(new SchedulerLog(groupName, jobName, tableName, SchedulerStatus.FAILED, e.getMessage()));
        }
    }
}
