package org.docero.data.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class LocalDateTimeAdapter extends XmlAdapter<String, LocalDateTime> {
    private final static Logger LOG = LoggerFactory.getLogger(LocalDateTimeAdapter.class);

    @Override
    public LocalDateTime unmarshal(String v) throws Exception {
        try {
            return v == null || v.trim().length() == 0 ? null :
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(v, LocalDateTime::from);
        } catch (DateTimeParseException dfe) {
            if (LOG.isDebugEnabled()) LOG.warn("can't parse date from '" + v + "'", dfe);
            return null;
        }
    }

    @Override
    public String marshal(LocalDateTime v) throws Exception {
        return v == null ? null : DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(v);
    }
}
