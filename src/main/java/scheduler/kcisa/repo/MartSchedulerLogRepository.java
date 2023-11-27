package scheduler.kcisa.repo;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.kcisa.model.mart.MartSchedulerLog;

import java.util.List;

@Repository
public interface MartSchedulerLogRepository extends JpaRepository<MartSchedulerLog, Long> {
    public List<MartSchedulerLog> findTop50ByOrderByCreatedAtDesc();

    public List<MartSchedulerLog> findTop50ByOrderByIdDesc();

    public MartSchedulerLog findByGroupAndName(String group, String name);
}
