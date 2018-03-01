package org.docero.data.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class LocalTimeAdapter extends XmlAdapter<String, LocalTime> {
    private final static Logger LOG = LoggerFactory.getLogger(LocalTimeAdapter.class);

    @Override
    public LocalTime unmarshal(String v) throws Exception {
        try {
            return v == null || v.trim().length() == 0 ? null :
                    DateTimeFormatter.ISO_LOCAL_TIME.parse(v, LocalTime::from);
        } catch (DateTimeParseException dfe) {
            if (LOG.isDebugEnabled()) LOG.warn("can't parse date from '" + v + "'", dfe);
            return null;
        }
    }

    @Override
    public String marshal(LocalTime v) throws Exception {
        return v == null ? null : DateTimeFormatter.ISO_LOCAL_TIME.format(v);
    }
}
