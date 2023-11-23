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
@Table(name = "ANALS_MTLY_FLAG", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"ANALS_MTLY_YM", "ANALS_MTLY_TABLE_NM"})
})
public class MonthlyAnalysisFlag {
    @javax.persistence.Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ANALS_MTLY_FLAG_SEQ_NO")
    private Long Id;
    @Column(name = "ANALS_MTLY_YM", length = 6)
    private String date;
    @Column(length = 200, name = "ANALS_MTLY_TABLE_NM", nullable = false)
    private String tableName;
    @Column(name = "ANALS_MTLY_FLAG", nullable = false)
    private boolean flag;
    @Column(name = "ANALS_DT", columnDefinition = "DATETIME")
    private LocalDate analysisDt = LocalDate.now();

    public MonthlyAnalysisFlag(String date, String tableName, boolean flag) {
        this.date = date;
        this.tableName = tableName;
        this.flag = flag;
    }
}
