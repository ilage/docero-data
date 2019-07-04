package org.docero.data.view;

import org.docero.data.utils.DDataException;

public class UnknownTableColumn extends DDataException {
    UnknownTableColumn(String s) {
        super(s);
    }
}
