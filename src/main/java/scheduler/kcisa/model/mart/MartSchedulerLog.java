package scheduler.kcisa.model.mart;

import lombok.*;
import scheduler.kcisa.model.SchedulerStatus;

import javax.persistence.*;
import java.util.Date;

@Getter
@ToString
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "mart_scheduler_log")
public class MartSchedulerLog {
    @Id
    @GeneratedValue(strategy = javax.persistence.GenerationType.IDENTITY)
    private Long id;
    @Column(length = 100, name = "group_name", nullable = false)
    private String group;
    @Column(length = 100, name = "job_name", nullable = false)
    private String name;
    @Column(length = 100, name = "table_name")
    private String table_name;
    @Enumerated(EnumType.STRING)
    @Column(length = 10)
    private SchedulerStatus status;
    @Column(name = "item_count")
    private Integer count;
    @Column(name = "crt_count")
    private Integer crt_count;
    @Column(name = "updt_count")
    private Integer updt_count;
    private String message;
    @Column(name = "created_at", nullable = false, updatable = false, columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdAt = new Date();

    public MartSchedulerLog(String group, String name, String table_name, SchedulerStatus status) {
        this.group = group;
        this.name = name;
        this.table_name = table_name;
        this.status = status;
    }

    public MartSchedulerLog(String group, String name, String table_name, SchedulerStatus status, String message) {
        this.group = group;
        this.name = name;
        this.table_name = table_name;
        this.status = status;
        this.message = message;
    }

    public MartSchedulerLog(String group, String name, String table_name, SchedulerStatus status, Integer count, Integer crt_count, Integer updt_count) {
        this.group = group;
        this.name = name;
        this.table_name = table_name;
        this.status = status;
        this.count = count;
        this.crt_count = crt_count;
        this.updt_count = updt_count;
    }

    public MartSchedulerLog(String group, String name, String table_name, SchedulerStatus status, Integer count) {
        this.group = group;
        this.name = name;
        this.table_name = table_name;
        this.status = status;
        this.count = count;
    }
}
