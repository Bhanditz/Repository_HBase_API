package org.bibalex.org.hbase.models;

import java.io.Serializable;

public class Association implements Serializable {
    //hbase key consists of: occurrenceId + targetOccurrenceId + associationType
    String associationId;
    String occurrenceId;
    String associationType;
    String targetOccurrenceId;
    String determinedDate;
    String determinedBy;
    String measurementMethod;
    String remarks;
    String source;
    String citation;
    String contributor;
    String referenceId;
    String deltaStatus;


    public String getDeltaStatus() {
        return deltaStatus;
    }

    public void setDeltaStatus(String deltaStatus) {
        this.deltaStatus = deltaStatus;
    }

    public String getAssociationId() {

        return associationId;
    }

    public void setAssociationId(String associationId) {
        this.associationId = associationId;
    }

    public String getOccurrenceId() {
        return occurrenceId;
    }

    public void setOccurrenceId(String occurrenceId) {
        this.occurrenceId = occurrenceId;
    }

    public String getAssociationType() {
        return associationType;
    }

    public void setAssociationType(String associationType) {
        this.associationType = associationType;
    }

    public String getTargetOccurrenceId() {
        return targetOccurrenceId;
    }

    public void setTargetOccurrenceId(String targetOccurrenceId) {
        this.targetOccurrenceId = targetOccurrenceId;
    }

    public String getDeterminedDate() {
        return determinedDate;
    }

    public void setDeterminedDate(String determinedDate) {
        this.determinedDate = determinedDate;
    }

    public String getDeterminedBy() {
        return determinedBy;
    }

    public void setDeterminedBy(String determinedBy) {
        this.determinedBy = determinedBy;
    }

    public String getMeasurementMethod() {
        return measurementMethod;
    }

    public void setMeasurementMethod(String measurementMethod) {
        this.measurementMethod = measurementMethod;
    }

    public String getRemarks() {
        return remarks;
    }

    public void setRemarks(String remarks) {
        this.remarks = remarks;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getCitation() {
        return citation;
    }

    public void setCitation(String citation) {
        this.citation = citation;
    }

    public String getContributor() {
        return contributor;
    }

    public void setContributor(String contributor) {
        this.contributor = contributor;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(String referenceId) {
        this.referenceId = referenceId;
    }
}
