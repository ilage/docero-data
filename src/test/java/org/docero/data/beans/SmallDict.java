package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DictionaryType;

@DDataBean(table = "smdict", schema = "ddata", dictionary = DictionaryType.SMALL)
public interface SmallDict extends SmallDictTable {
}
