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
import scheduler.kcisa.model.flag.collection.DailyCollectionFlag;
import scheduler.kcisa.service.SchedulerLogService;
import scheduler.kcisa.service.flag.collection.DailyCollectionFlagService;
import scheduler.kcisa.utils.JobUtils;
import scheduler.kcisa.utils.Utils;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Optional;

@Component
public class MobileCtgryUseQyInfo extends QuartzJobBean {
    String tableName = "colct_mobile_ctgry_use_qy_info";
    String url = "https://api.dmp.igaworks.com/v1/insight/category-usage";
    WebClient webClient = WebClient.builder().baseUrl(url).build();
    DailyCollectionFlagService flagService;
    @Value("${mobile.api.key}")
    String apiKey;

    public MobileCtgryUseQyInfo(DailyCollectionFlagService flagService) {
        this.flagService = flagService;
    }

    @Override
    protected void executeInternal(@NotNull JobExecutionContext context) throws JobExecutionException {
        LocalDate stdDate = LocalDate.now().minusDays(3);
        String date = stdDate.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        JobUtils.executeJob(context, tableName, jobData -> {
            Connection connection = jobData.conn;
            String groupName = jobData.groupName;
            String jobName = jobData.jobName;
            SchedulerLogService schedulerLogService = (SchedulerLogService) jobData.logService;
            int count = 0;

            String insertQuery = Utils.getSQLString("src/main/resources/sql/collection/mobile/MobileCtgryUseQy.sql");

            try (PreparedStatement preparedStatement = connection.prepareStatement(insertQuery);) {
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

                flagService.create(new DailyCollectionFlag(LocalDate.now(), tableName, true));
            }
        });
    }
}
