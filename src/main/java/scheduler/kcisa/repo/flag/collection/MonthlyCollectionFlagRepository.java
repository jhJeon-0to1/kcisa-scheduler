package scheduler.kcisa.repo.flag.collection;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.kcisa.model.flag.collection.DailyCollectionFlag;
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;

import java.time.LocalDate;

@Repository
public interface MonthlyCollectionFlagRepository extends JpaRepository<MonthlyCollectionFlag, Long> {
    MonthlyCollectionFlag findByDateAndTableName(String date, String tableName);
}
