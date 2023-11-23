package scheduler.kcisa.service.flag.analysis;

import org.springframework.stereotype.Service;
import scheduler.kcisa.model.flag.analysis.DailyAnalysisFlag;
import scheduler.kcisa.repo.flag.analysis.DailyAnalysisFlagRepository;

import java.time.LocalDate;

@Service
public class DailyAnalysisFlagService {
    private final DailyAnalysisFlagRepository repository;

    public DailyAnalysisFlagService(DailyAnalysisFlagRepository repository) {
        this.repository = repository;
    }

    public void create(DailyAnalysisFlag flag) {
        repository.save(flag);
    }

    public void update(DailyAnalysisFlag flag) {
        repository.save(flag);
    }

    public DailyAnalysisFlag findByDateAndTableName(LocalDate date, String tableName) {
        return repository.findByDateAndTableName(date, tableName);
    }
}
