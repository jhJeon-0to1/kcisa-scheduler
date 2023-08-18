package scheduler.test.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scheduler.test.model.SchedulerLog;
import scheduler.test.repo.SchedulerLogRepository;

@Service
public class SchedulerLogService {
    private final SchedulerLogRepository schedulerLogRepository;

    @Autowired
    public SchedulerLogService(SchedulerLogRepository schedulerLogRepository) {
        this.schedulerLogRepository = schedulerLogRepository;
    }

    public SchedulerLog create(SchedulerLog log) {
        return schedulerLogRepository.save(log);
    }

}
