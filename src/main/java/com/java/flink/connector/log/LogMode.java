package com.java.flink.connector.log;

public enum LogMode {
    STDOUT,LOG_INFO,LOG_WARN;

    public static LogMode fromName(String name) {
        for (LogMode mode : values()) {
            if (mode.name().equalsIgnoreCase(name)) {
                return mode;
            }
        }
        return null;
    }
}
