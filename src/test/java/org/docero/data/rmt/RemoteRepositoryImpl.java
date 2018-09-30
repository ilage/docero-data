package org.docero.data.rmt;

import java.util.HashMap;

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
        map.put(bean.getRemoteId(), bean);
        return bean;
    }

    @Override
    public RemoteBean update(RemoteBean bean) {
        map.put(bean.getRemoteId(), bean);
        return bean;
    }

    @Override
    public void delete(Integer id) {
        map.remove(id);
    }
}
