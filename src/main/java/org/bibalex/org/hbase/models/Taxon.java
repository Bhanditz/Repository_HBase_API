package org.bibalex.org.hbase.models;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class Taxon implements Serializable {
    String identifier;
    String scientificName;
    String parentTaxonId;
    String kingdom;
    String phylum;
    String class_;
    String order;
    String family;
    String genus;
    String taxonRank;
    String furtherInformationURI;
    String taxonomicStatus;
    String taxonRemarks;
    String namePublishedIn;
    String referenceId;
    String pageEolId;
    String acceptedNodeId;
    String source;
    String canonicalName;
    String scientificNameAuthorship;
    String scientificNameID;
    String datasetId;
    String eolIdAnnotations;
    String deltaStatus;
    ArrayList<String> guids;
    HashMap<String, String> guidsMapping;
    String taxonId;
//    String landmark;


    public String getTaxonId() {
        return taxonId;
    }

    public void setTaxonId(String taxonId) {
        this.taxonId = taxonId;
    }

    public HashMap<String, String> getGuidsMapping() {
        return guidsMapping;
    }

    public void setGuidsMapping(HashMap<String, String> guidsMapping) {
        this.guidsMapping = guidsMapping;
    }


    public ArrayList<String> getGuids() {
        return guids;
    }

    public void setGuids(ArrayList<String> guids) {
        this.guids = guids;
    }


    public String getDeltaStatus() {
        return deltaStatus;
    }

    public void setDeltaStatus(String deltaStatus) {
        this.deltaStatus = deltaStatus;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getCanonicalName() {
        return canonicalName;
    }

    public void setCanonicalName(String canonicalName) {
        this.canonicalName = canonicalName;
    }

    public String getScientificNameAuthorship() {
        return scientificNameAuthorship;
    }

    public void setScientificNameAuthorship(String scientificNameAuthorship) {
        this.scientificNameAuthorship = scientificNameAuthorship;
    }

    public String getScientificNameID() {
        return scientificNameID;
    }

    public void setScientificNameID(String scientificNameID) {
        this.scientificNameID = scientificNameID;
    }

    public String getDatasetId() {
        return datasetId;
    }

    public void setDatasetId(String datasetId) {
        this.datasetId = datasetId;
    }

    public String getEolIdAnnotations() {
        return eolIdAnnotations;
    }

    public void setEolIdAnnotations(String eolIdAnnotations) {
        this.eolIdAnnotations = eolIdAnnotations;
    }

    public String getAcceptedNodeId() {
        return acceptedNodeId;
    }

    public void setAcceptedNodeId(String acceptedNodeId) {
        this.acceptedNodeId = acceptedNodeId;
    }

    public String getIdentifier() {

        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public String getScientificName() {
        return scientificName;
    }

    public void setScientificName(String scientificName) {
        this.scientificName = scientificName;
    }

    public String getParentTaxonId() {
        return parentTaxonId;
    }

    public void setParentTaxonId(String parentTaxonId) {
        this.parentTaxonId = parentTaxonId;
    }

    public String getKingdom() {
        return kingdom;
    }

    public void setKingdom(String kingdom) {
        this.kingdom = kingdom;
    }

    public String getPhylum() {
        return phylum;
    }

    public void setPhylum(String phylum) {
        this.phylum = phylum;
    }

    public String getOrder() {
        return order;
    }

    public void setOrder(String order) {
        this.order = order;
    }

    public String getFamily() {
        return family;
    }

    public void setFamily(String family) {
        this.family = family;
    }

    public String getGenus() {
        return genus;
    }

    public void setGenus(String genus) {
        this.genus = genus;
    }

    public String getTaxonRank() {
        return taxonRank;
    }

    public void setTaxonRank(String taxonRank) {
        this.taxonRank = taxonRank;
    }

    public String getFurtherInformationURI() {
        return furtherInformationURI;
    }

    public void setFurtherInformationURI(String furtherInformationURI) {
        this.furtherInformationURI = furtherInformationURI;
    }

    public String getTaxonomicStatus() {
        return taxonomicStatus;
    }

    public void setTaxonomicStatus(String taxonomicStatus) {
        this.taxonomicStatus = taxonomicStatus;
    }

    public String getTaxonRemarks() {
        return taxonRemarks;
    }

    public void setTaxonRemarks(String taxonRemarks) {
        this.taxonRemarks = taxonRemarks;
    }

    public String getNamePublishedIn() {
        return namePublishedIn;
    }

    public void setNamePublishedIn(String namePublishedIn) {
        this.namePublishedIn = namePublishedIn;
    }

    public String getReferenceId() {
        return referenceId;
    }

    public void setReferenceId(String referenceId) {
        this.referenceId = referenceId;
    }

    public String getClass_() {
        return class_;
    }

    public void setClass_(String class_) {
        this.class_ = class_;
    }

    public String getPageEolId() {
        return pageEolId;
    }

    public void setPageEolId(String pageEolId) {
        this.pageEolId = pageEolId;
    }

//    public String getLandmark() { return landmark; }
//
//    public void setLandmark(String landmark) { this.landmark = landmark; }
}
