package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DictionaryType;

import java.util.List;

@DDataBean(table = "smdict", schema = "ddata", dictionary = DictionaryType.SMALL)
public interface SmallDict extends SmallDictTable {
    @SmallDict_Map_(value = SmallDict_.ID, graf = SmallDictGraf_.PARENT)
    List<SmallDictGraf> getGraf();
}
