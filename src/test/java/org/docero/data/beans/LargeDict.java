package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DictionaryType;

@DDataBean(table = "lgdict", schema = "ddata", dictionary = DictionaryType.LARGE)
public interface LargeDict extends LargeDictTable {
}
