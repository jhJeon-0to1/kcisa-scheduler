package scheduler.kcisa.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.kcisa.model.collection.SchedulerLog;

@Repository
public interface SchedulerLogRepository extends JpaRepository<SchedulerLog, Long> {
}
