package org.docero.data.rmt;

import org.docero.data.DDataRemoteRepository;

import javax.jws.WebMethod;

//@DDataRemote({RBean.class})
public interface RemoteRepository extends DDataRemoteRepository<RemoteBean, Integer> {
    @WebMethod
    @Override
    RemoteBean get(Integer id);

    @WebMethod
    @Override
    RemoteBean insert(RemoteBean bean);

    @WebMethod
    @Override
    RemoteBean update(RemoteBean bean);
}