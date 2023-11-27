package scheduler.kcisa.controller.log;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import scheduler.kcisa.model.collection.SchedulerLog;
import scheduler.kcisa.service.SchedulerLogService;

import java.util.List;

@Controller
@RequestMapping("/collection")
public class CollectionLogController {
    SchedulerLogService schedulerLogService;

    public CollectionLogController(SchedulerLogService schedulerLogService) {
        this.schedulerLogService = schedulerLogService;
    }

    @GetMapping("/log")
    public String log() {
        return "collection/log";
    }

    @GetMapping("/api/log")
    @ResponseBody
    public List<SchedulerLog> logs() {
//        최신순으로 정렬, 50개만 가져오기
        List<SchedulerLog> logs = schedulerLogService.findTop50ByOrderByIdDesc();

        if (logs != null) {
            return logs;
        }

        return null;
    }
}
