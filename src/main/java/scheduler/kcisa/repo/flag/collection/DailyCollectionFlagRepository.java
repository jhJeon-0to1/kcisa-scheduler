package scheduler.kcisa.repo.flag.collection;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.kcisa.model.flag.collection.DailyCollectionFlag;

import java.time.LocalDate;

@Repository
public interface DailyCollectionFlagRepository extends JpaRepository<DailyCollectionFlag, Long> {
    DailyCollectionFlag findByDateAndTableName(LocalDate date, String tableName);
}
