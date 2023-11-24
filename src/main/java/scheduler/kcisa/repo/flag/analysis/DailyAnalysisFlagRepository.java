package scheduler.kcisa.repo.flag.analysis;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.kcisa.model.flag.analysis.DailyAnalysisFlag;

@Repository
public interface DailyAnalysisFlagRepository extends JpaRepository<DailyAnalysisFlag, Long> {
    DailyAnalysisFlag findByDateAndTableName(String date, String tableName);
}
