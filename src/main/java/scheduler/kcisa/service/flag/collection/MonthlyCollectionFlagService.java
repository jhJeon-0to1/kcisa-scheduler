package scheduler.kcisa.service.flag.collection;

import org.springframework.stereotype.Service;
import scheduler.kcisa.model.flag.collection.MonthlyCollectionFlag;
import scheduler.kcisa.repo.flag.collection.MonthlyCollectionFlagRepository;

@Service
public class MonthlyCollectionFlagService {
    private final MonthlyCollectionFlagRepository repository;

    public MonthlyCollectionFlagService(MonthlyCollectionFlagRepository repository) {
        this.repository = repository;
    }

    public void create(MonthlyCollectionFlag flag) {
        repository.save(flag);
    }

    public void update(MonthlyCollectionFlag flag) {
        repository.save(flag);
    }

    public MonthlyCollectionFlag findByDateAndTableName(String date, String tableName) {
        return repository.findByDateAndTableName(date, tableName);
    }
}
