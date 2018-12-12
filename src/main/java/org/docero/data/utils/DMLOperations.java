package org.docero.data.utils;

import java.time.ZoneId;

public class DMLOperations implements AutoCloseable {
    private static class DMLOptions {
        java.time.LocalDateTime temporal;
        boolean updatesWithoutRead;
    }

    private final static ThreadLocal<DMLOptions> options = new ThreadLocal<>();

    public DMLOperations() {
        options.set(new DMLOptions());
    }

    public DMLOperations setLocalDateTime(java.time.LocalDateTime v) {
        options.get().temporal = v;
        return this;
    }

    public DMLOperations setFastUpdates() {
        options.get().updatesWithoutRead = true;
        return this;
    }

    @Override
    public void close() {
        options.remove();
    }



    public static java.time.LocalDateTime localDateTime() {
        DMLOptions dml = options.get();
        return dml == null ? java.time.LocalDateTime.now() : dml.temporal;
    }

    public static java.time.LocalDate localDate() {
        DMLOptions dml = options.get();
        return dml == null ? java.time.LocalDate.now() : dml.temporal.toLocalDate();
    }

    public static java.sql.Timestamp timeStamp() {
        DMLOptions dml = options.get();
        return dml == null ?
                new java.sql.Timestamp(System.currentTimeMillis()) :
                java.sql.Timestamp.valueOf(dml.temporal);
    }

    public static java.util.Date date() {
        DMLOptions dml = options.get();
        return dml == null ? new java.util.Date() :
                java.util.Date.from(dml.temporal.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static boolean updateWithoutRead() {
        DMLOptions dml = options.get();
        return dml != null && dml.updatesWithoutRead;
    }
}
