package org.docero.data.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class LocalDateAdapter extends XmlAdapter<String, LocalDate> {
    private final static Logger LOG = LoggerFactory.getLogger(LocalDateAdapter.class);

    @Override
    public LocalDate unmarshal(String v) throws Exception {
        try {
            return v == null || v.trim().length() == 0 ? null :
                    DateTimeFormatter.ISO_LOCAL_DATE.parse(v, LocalDate::from);
        } catch (DateTimeParseException dfe) {
            if (LOG.isDebugEnabled()) LOG.warn("can't parse date from '" + v + "'", dfe);
            return null;
        }
    }

    @Override
    public String marshal(LocalDate v) throws Exception {
        return v == null ? null : DateTimeFormatter.ISO_LOCAL_DATE.format(v);
    }
}
