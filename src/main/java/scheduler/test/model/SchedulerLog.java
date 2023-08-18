package scheduler.test.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.Date;

@Getter
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "scheduler_log")
public class SchedulerLog {
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
    private String message;
    @Column(name = "created_at", nullable = false, updatable = false, columnDefinition = "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
    @Temporal(TemporalType.TIMESTAMP)
    private Date createdAt = new Date();

    public SchedulerLog(String group, String name, String table_name ,SchedulerStatus status) {
        this.group = group;
        this.name = name;
        this.table_name = table_name;
        this.status = status;
    }

    public SchedulerLog(String group, String name, String table_name, SchedulerStatus status, String message) {
        this.group = group;
        this.name = name;
        this.table_name = table_name;
        this.status = status;
        this.message = message;
    }

    public SchedulerLog(String group, String name, String table_name, SchedulerStatus status, Integer count) {
        this.group = group;
        this.name = name;
        this.table_name = table_name;
        this.status = status;
        this.count = count;
    }
}
