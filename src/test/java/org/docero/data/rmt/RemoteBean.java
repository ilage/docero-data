package org.docero.data.rmt;

public class RemoteBean implements RBean {
    private int remoteId;
    private String name;

    public int getRemoteId() {
        return remoteId;
    }

    public void setRemoteId(int remoteId) {
        this.remoteId = remoteId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
