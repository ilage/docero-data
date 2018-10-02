package org.docero.data.rmt;
/*
    Class generated by docero-data processor.
*/
@javax.xml.bind.annotation.XmlRootElement(name="Remote")
@javax.xml.bind.annotation.XmlAccessorType(javax.xml.bind.annotation.XmlAccessType.PROPERTY)
@com.fasterxml.jackson.annotation.JsonIgnoreProperties({"handler","ddataBeanKey_"})
@org.docero.data.remote.DDataPrototypeRealization
public class RemoteBean implements org.docero.data.rmt.Remote {
    private int remoteId;
    private java.lang.String name;

    public int getRemoteId() {
        return remoteId;
    }

    public void setRemoteId(int remoteId) {
        this.remoteId = remoteId;
    }

    public java.lang.String getName() {
        return name;
    }

    public void setName(java.lang.String name) {
        this.name = name;
    }

    private int hash_;
    final public int hashCode() {
        if(hash_ == 0) {
            int h = 0;
            h += java.lang.Integer.hashCode(remoteId);
            hash_ = h;
        }
        return hash_;
    }

    final public boolean equals(Object o) {
        return o!=null && o instanceof org.docero.data.rmt.Remote && remoteId==((org.docero.data.rmt.Remote)o).getRemoteId();
    }

    public int compareTo(org.docero.data.rmt.Remote o) {
        if (o == null) throw new NullPointerException();
        int r = compare(this.getName(), o.getName());
        return r;
    }

    public int compareSimpleTypes(org.docero.data.rmt.Remote o) {
        if (o == null) throw new NullPointerException();
        int r = compare(this.getName(), o.getName());
        return r;
    }
    private int compare(Object o1, Object o2) {
        if (o1 == null) return o2 == null ? 0 : -1;
        if (o2 == null) return 1;
        if (o1 instanceof java.lang.Comparable)
          return ((java.lang.Comparable) o1).compareTo(o2);
        else return 0;
    }
}
