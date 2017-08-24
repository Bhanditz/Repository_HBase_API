package org.bibalex.org.hbase.models;

import java.io.Serializable;

public class Event implements Serializable {
    String eventId;
    String locality;
    String remarks;


    public String getEventId() {

        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getLocality() {
        return locality;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }
}
