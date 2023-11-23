package scheduler.kcisa.service.flag.analysis;

import org.springframework.stereotype.Service;
import scheduler.kcisa.model.flag.analysis.YearlyAnalysisFlag;
import scheduler.kcisa.repo.flag.analysis.YearlyAnalysisFlagRepository;

@Service
public class YearlyAnalysisFlagService {
    private final YearlyAnalysisFlagRepository repository;

    public YearlyAnalysisFlagService(YearlyAnalysisFlagRepository repository) {
        this.repository = repository;
    }

    public void create(YearlyAnalysisFlag flag) {
        repository.save(flag);
    }

    public void update(YearlyAnalysisFlag flag) {
        repository.save(flag);
    }

    public YearlyAnalysisFlag findByDateAndTableName(String date, String tableName) {
        return repository.findByDateAndTableName(date, tableName);
    }
}
