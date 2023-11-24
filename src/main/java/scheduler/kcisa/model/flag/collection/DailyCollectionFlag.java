package scheduler.kcisa.model.flag.collection;

import lombok.*;

import javax.persistence.*;
import java.time.LocalDate;

@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "COLCT_DALY_FLAG", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"COLCT_DALY_DE", "COLCT_DALY_TABLE_NM"})
})
public class DailyCollectionFlag {
    @Id
    @GeneratedValue(strategy = javax.persistence.GenerationType.IDENTITY)
    @Column(name = "COLCT_DALY_FLAG_SEQ_NO")
    private Long Id;
    @Column(name = "COLCT_DALY_DE", length = 8)
    private String date;
    @Column(length = 200, name = "COLCT_DALY_TABLE_NM", nullable = false)
    private String tableName;
    @Column(name = "COLCT_DALY_FLAG", nullable = false)
    private boolean flag;
    @Column(name = "COLCT_DT", columnDefinition = "DATETIME")
    private LocalDate colctDt = LocalDate.now();

    public DailyCollectionFlag(String date, String tableName, boolean flag) {
        this.date = date;
        this.tableName = tableName;
        this.flag = flag;
    }
}
