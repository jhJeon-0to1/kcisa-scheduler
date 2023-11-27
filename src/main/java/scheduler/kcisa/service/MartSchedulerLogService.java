package scheduler.kcisa.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scheduler.kcisa.model.mart.MartSchedulerLog;
import scheduler.kcisa.repo.MartSchedulerLogRepository;

import java.util.List;

@Service
public class MartSchedulerLogService implements LogService<MartSchedulerLog> {
    private final MartSchedulerLogRepository martSchedulerLogRepository;

    @Autowired
    public MartSchedulerLogService(MartSchedulerLogRepository martSchedulerLogRepository) {
        this.martSchedulerLogRepository = martSchedulerLogRepository;
    }

    public MartSchedulerLog create(MartSchedulerLog log) {
        return martSchedulerLogRepository.save(log);
    }

    public List<MartSchedulerLog> findTop50ByOrderByCreatedAtDesc() {
        return martSchedulerLogRepository.findTop50ByOrderByCreatedAtDesc();
    }

    public List<MartSchedulerLog> findTop50ByOrderByIdDesc() {
        return martSchedulerLogRepository.findTop50ByOrderByIdDesc();
    }

    public MartSchedulerLog findByGroupAndName(String group, String name) {
        return martSchedulerLogRepository.findByGroupAndName(group, name);
    }
}
