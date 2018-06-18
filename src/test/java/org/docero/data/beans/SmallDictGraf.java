package org.docero.data.beans;

import org.docero.data.DDataBean;
import org.docero.data.DDataProperty;

import java.io.Serializable;

@DDataBean(table = "smgraf", schema = "ddata")
public interface SmallDictGraf extends Serializable {
    @DDataProperty(id = true)
    int getParent();
    void setParent(int id);

    @DDataProperty(id = true)
    int getChild();
    void setChild(int id);

    @SmallDictGraf_Map_(value = SmallDictGraf_.CHILD, linked = SmallDict_.ID)
    SmallDict getLinked();
}
