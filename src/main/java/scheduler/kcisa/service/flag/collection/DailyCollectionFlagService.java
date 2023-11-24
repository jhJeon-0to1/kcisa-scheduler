package scheduler.kcisa.service.flag.collection;

import org.springframework.stereotype.Service;
import scheduler.kcisa.model.flag.collection.DailyCollectionFlag;
import scheduler.kcisa.repo.flag.collection.DailyCollectionFlagRepository;

@Service
public class DailyCollectionFlagService {
    private final DailyCollectionFlagRepository dailyCollectionFlagRepository;

    public DailyCollectionFlagService(DailyCollectionFlagRepository dailyCollectionFlagRepository) {
        this.dailyCollectionFlagRepository = dailyCollectionFlagRepository;
    }

    public void create(DailyCollectionFlag flag) {
        dailyCollectionFlagRepository.save(flag);
    }

    public void update(DailyCollectionFlag flag) {
        dailyCollectionFlagRepository.save(flag);
    }

    public DailyCollectionFlag findByDateAndTableName(String date, String tableName) {
        return dailyCollectionFlagRepository.findByDateAndTableName(date, tableName);
    }
}
