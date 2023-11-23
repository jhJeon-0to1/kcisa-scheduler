package scheduler.kcisa.utils;

public enum ScheduleInterval {
    DAILY("daily"),
    MONTHLY("monthly"),
    YEARLY("yearly");

    private final String value;

    ScheduleInterval(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
