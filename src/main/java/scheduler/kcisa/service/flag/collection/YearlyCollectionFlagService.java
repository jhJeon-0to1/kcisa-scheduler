package scheduler.kcisa.service.flag.collection;

import org.springframework.stereotype.Service;
import scheduler.kcisa.model.flag.collection.YearlyCollectionFlag;
import scheduler.kcisa.repo.flag.collection.YearlyCollectionFlagRepository;

@Service
public class YearlyCollectionFlagService {
    private final YearlyCollectionFlagRepository repository;

    public YearlyCollectionFlagService(YearlyCollectionFlagRepository repository) {
        this.repository = repository;
    }

    public void create(YearlyCollectionFlag flag) {
        repository.save(flag);
    }

    public void update(YearlyCollectionFlag flag) {
        repository.save(flag);
    }

    public YearlyCollectionFlag findByDateAndTableName(String date, String tableName) {
        return repository.findByDateAndTableName(date, tableName);
    }
}
