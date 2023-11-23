package scheduler.kcisa.model.flag.analysis;

import lombok.*;

import javax.persistence.*;
import java.time.LocalDate;

@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "ANALS_DALY_FLAG", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"ANALS_DALY_DE", "ANALS_DALY_TABLE_NM"})
})
public class DailyAnalysisFlag {
    @javax.persistence.Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ANALS_DALY_FLAG_SEQ_NO")
    private Long Id;
    @Column(name = "ANALS_DALY_DE", length = 6)
    private LocalDate date;
    @Column(length = 200, name = "ANALS_DALY_TABLE_NM", nullable = false)
    private String tableName;
    @Column(name = "ANALS_DALY_FLAG", nullable = false)
    private boolean flag;
    @Column(name = "ANALS_DT", columnDefinition = "DATETIME")
    private LocalDate analysisDt = LocalDate.now();

    public DailyAnalysisFlag(LocalDate date, String tableName, boolean flag) {
        this.date = date;
        this.tableName = tableName;
        this.flag = flag;
    }
}
