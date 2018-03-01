package org.docero.data.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class OffsetDateTimeAdapter extends XmlAdapter<String, OffsetDateTime> {
    private final static Logger LOG = LoggerFactory.getLogger(OffsetDateTimeAdapter.class);

    @Override
    public OffsetDateTime unmarshal(String v) throws Exception {
        try {
            return v == null || v.trim().length() == 0 ? null :
                    DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(v, OffsetDateTime::from);
        } catch (DateTimeParseException dfe) {
            if (LOG.isDebugEnabled()) LOG.warn("can't parse date from '" + v + "'", dfe);
            return null;
        }
    }

    @Override
    public String marshal(OffsetDateTime v) throws Exception {
        return v == null ? null : DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(v);
    }
}
