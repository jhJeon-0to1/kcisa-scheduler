package scheduler.kcisa.controller;

import org.quartz.*;
import org.quartz.impl.matchers.GroupMatcher;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import scheduler.kcisa.model.JobInfo;
import scheduler.kcisa.model.SchedulerType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Controller
public class MainController {
    private Scheduler scheduler;

    public MainController(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @GetMapping("/")
    public String index() {
        return "index";
    }

    @GetMapping("/api/jobs")
    @ResponseBody
    public List<JobInfo> jobInfo() throws SchedulerException {
        List<JobInfo> jobs = new ArrayList<>();
        Set<JobKey> jobKeys = scheduler.getJobKeys(GroupMatcher.anyGroup());
        for (JobKey jobKey : jobKeys) {
            List<? extends Trigger> triggers = scheduler.getTriggersOfJob(jobKey);
            Trigger trigger = triggers.get(0);

            CronTrigger cronTrigger = (CronTrigger) trigger;
            JobInfo jobInfo = new JobInfo(jobKey.getGroup(), jobKey.getName(), SchedulerType.CRON, cronTrigger.getCronExpression());
            jobs.add(jobInfo);

        }

        return jobs;
    }

    @PostMapping("/api/jobs/simple")
    @ResponseBody
    public ResponseEntity<?> createJob(@RequestBody JobRequest request) throws SchedulerException {
// 이미 있는 job을 찾아서 그 job에 단발성 트리거를 새로 추가
        JobDetail jobDetail = scheduler.getJobDetail(new JobKey(request.jobName, request.groupName));
        if (jobDetail != null) {
            SimpleTrigger trigger = (SimpleTrigger) TriggerBuilder.newTrigger()
                    .forJob(jobDetail)
                    .withIdentity(request.jobName + "--one-time", request.groupName)
                    .startNow()
                    .build();
            scheduler.scheduleJob(trigger);
        }

        return ResponseEntity.ok().build();
    }


    public static class JobRequest {
        public String groupName;
        public String jobName;
    }
}
