package com.java.flink.gene;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class TimestampFormatedGene extends AbstractFieldGene<String> {
    // 标准日期时间格式，精确到秒：yyyy-MM-dd HH:mm:ss
    public static final String NORM_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    public static final DateTimeFormatter NORM_DATETIME_FORMATTER = DateTimeFormatter.ofPattern(NORM_DATETIME_PATTERN);
    // 标准日期时间格式，精确到毫秒：yyyy-MM-dd HH:mm:ss.SSS
    public static final String NORM_DATETIME_MS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final DateTimeFormatter NORM_DATETIME_MS_FORMATTER = DateTimeFormatter.ofPattern(NORM_DATETIME_MS_PATTERN);

    private final String format;
    private final DateTimeFormatter formatter;


    public TimestampFormatedGene(String fieldName) {
        this(fieldName, NORM_DATETIME_PATTERN);
    }

    public TimestampFormatedGene(String fieldName, String format) {
        super(fieldName);
        assert format != null;
        this.format = format;
        switch (format) {
            case NORM_DATETIME_PATTERN:
                this.formatter = NORM_DATETIME_FORMATTER;
                break;
            case NORM_DATETIME_MS_PATTERN:
                this.formatter = NORM_DATETIME_MS_FORMATTER;
                break;
            default:
                this.formatter = DateTimeFormatter.ofPattern(format);;
        }

    }

    @Override
    public String geneValue() throws Exception {
        LocalDateTime dateTime = LocalDateTime.now();
        return dateTime.format(NORM_DATETIME_FORMATTER);
    }
}
