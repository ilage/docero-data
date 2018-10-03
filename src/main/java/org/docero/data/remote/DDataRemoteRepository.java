package org.docero.data.remote;

import javax.jws.WebMethod;
import javax.jws.WebParam;
import javax.xml.bind.annotation.XmlTransient;

@XmlTransient
public interface DDataRemoteRepository<T, C> {
    @WebMethod T get(@WebParam C id);
    @WebMethod T insert(T bean);
    @WebMethod T update(T bean);
    @WebMethod void delete(@WebParam C id);
}
