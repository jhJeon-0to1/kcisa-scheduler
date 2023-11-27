package scheduler.kcisa.controller.log;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.service.MartSchedulerLogService;

import java.util.List;

@Controller
@RequestMapping("/analysis")
public class AnalysisLogController {
    MartSchedulerLogService schedulerLogService;

    public AnalysisLogController(MartSchedulerLogService schedulerLogService) {
        this.schedulerLogService = schedulerLogService;
    }

    @GetMapping("/log")
    public String log() {
        return "analysis/log";
    }

    @GetMapping("/api/log")
    @ResponseBody
    public List<MartSchedulerLog> logs() {
//        최신순으로 정렬, 50개만 가져오기
        List<MartSchedulerLog> logs = schedulerLogService.findTop50ByOrderByIdDesc();

        if (logs != null) {
            return logs;
        }

        return null;
    }
}
