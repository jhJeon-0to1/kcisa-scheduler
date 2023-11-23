package scheduler.kcisa.repo.flag.collection;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.kcisa.model.flag.collection.YearlyCollectionFlag;

@Repository
public interface YearlyCollectionFlagRepository extends JpaRepository<YearlyCollectionFlag, Long> {
    YearlyCollectionFlag findByDateAndTableName(String date, String tableName);
}
