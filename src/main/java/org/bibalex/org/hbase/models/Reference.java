package org.bibalex.org.hbase.models;

import java.io.Serializable;

/**
 * As this is the harvester, it will put all the fields in HBase. This, in turn, will be transformed to bytes array. No need for casting
 * because of that.
 */
public class Reference implements Serializable {
   // used as hbase key of a reference


    String referenceId;
    String publicationType;
    String fullReference;
    String primaryTitle;
    String secondaryTitle;
    String pages;
    String pageStart;
    String pageEnd;
    String volume;
    String edition;
    String publisher;
    String authorsList;
    String editorsList;
    String dateCreated;
    String language;
    String url;
    String doi;
    String localityOfPublisher;
    String deltaStatus;

    public String getDeltaStatus() {
        return deltaStatus;
    }

    public void setDeltaStatus(String deltaStatus) {
        this.deltaStatus = deltaStatus;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(String referenceId) {
        this.referenceId = referenceId;
    }

    public String getPublicationType() {
        return publicationType;
    }

    public void setPublicationType(String publicationType) {
        this.publicationType = publicationType;
    }

    public String getFullReference() {
        return fullReference;
    }

    public void setFullReference(String fullReference) {
        this.fullReference = fullReference;
    }

    public String getPrimaryTitle() {
        return primaryTitle;
    }

    public void setPrimaryTitle(String primaryTitle) {
        this.primaryTitle = primaryTitle;
    }

    public String getSecondaryTitle() {
        return secondaryTitle;
    }

    public void setSecondaryTitle(String secondaryTitle) {
        this.secondaryTitle = secondaryTitle;
    }

    public String getPages() {
        return pages;
    }

    public void setPages(String pages) {
        this.pages = pages;
    }

    public String getPageStart() {
        return pageStart;
    }

    public void setPageStart(String pageStart) {
        this.pageStart = pageStart;
    }

    public String getPageEnd() {
        return pageEnd;
    }

    public void setPageEnd(String pageEnd) {
        this.pageEnd = pageEnd;
    }

    public String getVolume() {
        return volume;
    }

    public void setVolume(String volume) {
        this.volume = volume;
    }

    public String getEdition() {
        return edition;
    }

    public void setEdition(String edition) {
        this.edition = edition;
    }

    public String getPublisher() {
        return publisher;
    }

    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public String getAuthorsList() {
        return authorsList;
    }

    public void setAuthorsList(String authorsList) {
        this.authorsList = authorsList;
    }

    public String getEditorsList() {
        return editorsList;
    }

    public void setEditorsList(String editorsList) {
        this.editorsList = editorsList;
    }

    public String getDateCreated() {
        return dateCreated;
    }

    public void setDateCreated(String dateCreated) {
        this.dateCreated = dateCreated;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getDoi() {
        return doi;
    }

    public void setDoi(String doi) {
        this.doi = doi;
    }

    public String getLocalityOfPublisher() {
        return localityOfPublisher;
    }

    public void setLocalityOfPublisher(String localityOfPublisher) {
        this.localityOfPublisher = localityOfPublisher;
    }
}
