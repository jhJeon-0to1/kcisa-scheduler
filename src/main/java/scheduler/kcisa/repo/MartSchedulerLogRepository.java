package scheduler.kcisa.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.kcisa.model.mart.MartSchedulerLog;

@Repository
public interface MartSchedulerLogRepository extends JpaRepository<MartSchedulerLog, Long> {
}
