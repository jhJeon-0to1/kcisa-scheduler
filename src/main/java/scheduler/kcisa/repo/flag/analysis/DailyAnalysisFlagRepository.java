package scheduler.kcisa.repo.flag.analysis;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import scheduler.kcisa.model.flag.analysis.DailyAnalysisFlag;

import java.time.LocalDate;

@Repository
public interface DailyAnalysisFlagRepository extends JpaRepository<DailyAnalysisFlag, Long> {
    DailyAnalysisFlag findByDateAndTableName(LocalDate date, String tableName);
}
