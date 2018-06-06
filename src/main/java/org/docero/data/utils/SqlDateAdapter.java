package org.docero.data.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.annotation.adapters.XmlAdapter;
import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

public class SqlDateAdapter extends XmlAdapter<String, Date> {
    private final static Logger LOG = LoggerFactory.getLogger(SqlDateAdapter.class);

    @Override
    public Date unmarshal(String v) throws Exception {
        try {
            return v == null || v.trim().length() == 0 ? null :
                    Date.valueOf(DateTimeFormatter.ISO_LOCAL_DATE.parse(v, LocalDate::from));
        } catch (DateTimeParseException dfe) {
            if (LOG.isDebugEnabled()) LOG.warn("can't parse date from '" + v + "'", dfe);
            return null;
        }
    }

    @Override
    public String marshal(Date v) throws Exception {
        return v == null ? null : DateTimeFormatter.ISO_LOCAL_DATE.format(v.toLocalDate());
    }
}
