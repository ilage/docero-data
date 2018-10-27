package org.docero.data.utils;

import java.time.ZoneId;

public class TemporalDataOperations {
    public static class LocalDateTime implements AutoCloseable {
        private final static ThreadLocal<java.time.LocalDateTime> temporal = new ThreadLocal<>();

        public LocalDateTime(java.time.LocalDateTime val) {
            temporal.set(val);
        }

        @Override
        public void close() {
            temporal.remove();
        }

        public static java.time.LocalDateTime get() {
            java.time.LocalDateTime ts = temporal.get();
            return ts == null ? java.time.LocalDateTime.now() : ts;
        }
    }

    public static class LocalDate implements AutoCloseable {
        private final static ThreadLocal<java.time.LocalDate> temporal = new ThreadLocal<>();

        public LocalDate(java.time.LocalDate val) {
            temporal.set(val);
        }

        @Override
        public void close() {
            temporal.remove();
        }

        public static java.time.LocalDate get() {
            java.time.LocalDate ts = temporal.get();
            return ts == null ? java.time.LocalDate.now() : ts;
        }
    }

    public static class Timestamp implements AutoCloseable {
        private final static ThreadLocal<java.sql.Timestamp> temporal = new ThreadLocal<>();

        public Timestamp(java.sql.Timestamp val) {
            temporal.set(val);
        }

        @Override
        public void close() {
            temporal.remove();
        }

        public static java.sql.Timestamp get() {
            java.sql.Timestamp ts = temporal.get();
            return ts == null ? new java.sql.Timestamp(System.currentTimeMillis()) : ts;
        }
    }

    public static class Date implements AutoCloseable {
        private final static ThreadLocal<java.util.Date> temporal = new ThreadLocal<>();

        public Date(java.util.Date val) {
            temporal.set(val);
        }

        public Date(java.time.LocalDateTime val) {
            temporal.set(java.util.Date.from(val.atZone(ZoneId.systemDefault()).toInstant()));
        }

        @Override
        public void close() {
            temporal.remove();
        }

        public static java.util.Date get() {
            java.util.Date ts = temporal.get();
            return ts == null ? new java.util.Date() : ts;
        }
    }
}
