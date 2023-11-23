package scheduler.kcisa.repo.flag.analysis;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.kcisa.model.flag.analysis.MonthlyAnalysisFlag;

@Repository
public interface MonthlyAnalysisFlagRepository extends JpaRepository<MonthlyAnalysisFlag, Long> {
    MonthlyAnalysisFlag findByDateAndTableName(String date, String tableName);
}
