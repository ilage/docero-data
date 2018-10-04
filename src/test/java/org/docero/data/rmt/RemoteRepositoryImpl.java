package org.docero.data.rmt;

import org.docero.data.DictionaryType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class RemoteRepositoryImpl implements RemoteRepository {
    private final HashMap<Integer, RemoteBean> map =
            new HashMap<Integer, RemoteBean>() {{
                RemoteBean b = new RemoteBean() {{
                    setRemoteId(1);
                    setName("first");
                }};
                put(b.getRemoteId(), b);
                b = new RemoteBean() {{
                    setRemoteId(2);
                    setName("second");
                }};
                put(b.getRemoteId(), b);
            }};

    @Override
    public RemoteBean get(Integer id) {
        return map.get(id);
    }

    @Override
    public RemoteBean insert(RemoteBean bean) {
        version = (int) System.currentTimeMillis();
        map.put(bean.getRemoteId(), bean);
        return bean;
    }

    @Override
    public RemoteBean update(RemoteBean bean) {
        version = (int) System.currentTimeMillis();
        map.put(bean.getRemoteId(), bean);
        return bean;
    }

    @Override
    public void delete(Integer id) {
        version = (int) System.currentTimeMillis();
        map.remove(id);
    }

    @Override
    public DictionaryType getDictionaryType() {
        return DictionaryType.SMALL;
    }

    private int version = (int) System.currentTimeMillis();
    @Override
    public Integer version_() {
        return version;
    }

    @Override
    public List<RemoteBean> list() {
        return new ArrayList<>(map.values());
    }
}
