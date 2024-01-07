package com.java.flink.util;

import java.time.*;

/**
 * 时间戳工具类, 主要操作时间戳, 大数据量时使用int/long比使用时间对象高效
 */
public class TsUtils {
    public final static long SECOND_UNIT = 1000L;
    public final static long MINUTE_UNIT = 1000L * 60;
    public final static long MINUTE_UNIT5 = 1000L * 60 * 5;
    public final static long MINUTE_UNIT10 = 1000L * 60 * 10;
    public final static long HOUR_UNIT = 1000L * 60 * 60;
    public final static long DAY_UNIT = 1000L * 60 * 60 * 24;

    /**
     * The number of days in a 400 year cycle.
     */
    private final static int DAYS_PER_CYCLE = 146097;
    /**
     * The number of days from year zero to year 1970.
     * There are five 400 year cycles from year zero to 2000.
     * There are 7 leap years from 1970 to 2000.
     */
    private final static int DAYS_0000_TO_1970 = (DAYS_PER_CYCLE * 5) - (30 * 365 + 7);

    private final static long START_TS_1970 = 0;

    /* 时间戳规约到整点整分 */

    public static long tsIntervalMin(long ts, int intervalMin) {
        long interval = MINUTE_UNIT * intervalMin;
        return ts / interval * interval;
    }

    public static long tsIntervalMin(long ts) {
        return ts / MINUTE_UNIT * MINUTE_UNIT;
    }

    public static long tsInterval5Min(long ts) {
        return ts / MINUTE_UNIT5 * MINUTE_UNIT5;
    }

    public static long tsInterval10Min(long ts) {
        return ts / MINUTE_UNIT10 * MINUTE_UNIT10;
    }

    public static long tsIntervalHour(long ts) {
        return ts / HOUR_UNIT * HOUR_UNIT;
    }

    public static int daysFrom1970(long ts) {
        return (int) Instant.ofEpochMilli(ts).atZone(ZoneId.systemDefault()).toLocalDate().toEpochDay();
    }

    public static int daysFrom1970(long ts, ZoneId zone) {
        return (int) Instant.ofEpochMilli(ts).atZone(zone).toLocalDate().toEpochDay();
    }

    public static int daysFrom1970_2(long ts) {
        //LocalDate.from();
        LocalDate.ofEpochDay(1).toEpochDay();
        LocalDateTime.now();
        ZonedDateTime.ofInstant(Instant.now(), ZoneId.of("Asia/Shanghai"));
        return 0;
    }

}
