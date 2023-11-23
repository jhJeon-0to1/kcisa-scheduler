package scheduler.kcisa.service.flag.analysis;

import org.springframework.stereotype.Service;
import scheduler.kcisa.model.flag.analysis.MonthlyAnalysisFlag;
import scheduler.kcisa.repo.flag.analysis.MonthlyAnalysisFlagRepository;

@Service
public class MonthlyAnalysisFlagService {
    private final MonthlyAnalysisFlagRepository repository;

    public MonthlyAnalysisFlagService(MonthlyAnalysisFlagRepository repository) {
        this.repository = repository;
    }

    public void create(MonthlyAnalysisFlag flag) {
        repository.save(flag);
    }

    public void update(MonthlyAnalysisFlag flag) {
        repository.save(flag);
    }

    public MonthlyAnalysisFlag findByDateAndTableName(String date, String tableName) {
        return repository.findByDateAndTableName(date, tableName);
    }
}
