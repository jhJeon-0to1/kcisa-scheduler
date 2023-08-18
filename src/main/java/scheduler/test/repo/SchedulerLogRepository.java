package scheduler.test.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.test.model.SchedulerLog;

@Repository
public interface SchedulerLogRepository extends JpaRepository<SchedulerLog, Long> {
}
