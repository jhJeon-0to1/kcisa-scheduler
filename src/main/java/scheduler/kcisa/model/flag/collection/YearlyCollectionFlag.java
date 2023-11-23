package scheduler.kcisa.model.flag.collection;

import lombok.*;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Table;
import java.time.LocalDate;

@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "COLCT_YEARLY_FLAG", uniqueConstraints = {
        @javax.persistence.UniqueConstraint(columnNames = {"COLCT_YEARLY_YEAR", "COLCT_YEARLY_TABLE_NM"})
})
public class YearlyCollectionFlag {
    @javax.persistence.Id
    @GeneratedValue(strategy = javax.persistence.GenerationType.IDENTITY)
    @Column(name = "COLCT_YEARLY_FLAG_SEQ_NO")
    private Long Id;
    @Column(name = "COLCT_YEARLY_YEAR", length = 4)
    private String date;
    @Column(length = 200, name = "COLCT_YEARLY_TABLE_NM", nullable = false)
    private String tableName;
    @Column(name = "COLCT_YEARLY_FLAG", nullable = false)
    private boolean flag;
    @Column(name = "COLCT_DT", columnDefinition = "DATETIME")
    private LocalDate colctDt = LocalDate.now();

    public YearlyCollectionFlag(String date, String tableName, boolean flag) {
        this.date = date;
        this.tableName = tableName;
        this.flag = flag;
    }
}
