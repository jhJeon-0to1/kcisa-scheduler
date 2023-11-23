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
@Table(name = "COLCT_MTLY_FLAG", uniqueConstraints = {
        @UniqueConstraint(columnNames = {"COLCT_MTLY_YM", "COLCT_MTLY_TABLE_NM"})
})
public class MonthlyCollectionFlag {
    @javax.persistence.Id
    @GeneratedValue(strategy = javax.persistence.GenerationType.IDENTITY)
    @Column(name = "COLCT_MTLY_FLAG_SEQ_NO")
    private Long Id;
    @Column(name = "COLCT_MTLY_YM", length = 6)
    private String date;
    @Column(length = 200, name = "COLCT_MTLY_TABLE_NM", nullable = false)
    private String tableName;
    @Column(name = "COLCT_MTLY_FLAG", nullable = false)
    private boolean flag;
    @Column(name = "COLCT_DT", columnDefinition = "DATETIME")
    private LocalDate colctDt = LocalDate.now();

    public MonthlyCollectionFlag(String date, String tableName, boolean flag) {
        this.date = date;
        this.tableName = tableName;
        this.flag = flag;
    }
}
