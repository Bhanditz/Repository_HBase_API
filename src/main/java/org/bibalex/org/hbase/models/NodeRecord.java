package org.bibalex.org.hbase.models;

import java.io.Serializable;
import java.util.ArrayList;

/*
This class will include the needed objects and attributes
 */
public class NodeRecord implements Serializable {
    int resourceId;
    ArrayList<VernacularName> vernaculars;
    ArrayList<Reference> references;
    ArrayList<Occurrence> occurrences;
    ArrayList<Association> associations;
    ArrayList<MeasurementOrFact> measurementOrFacts;
    ArrayList<Media> media;
    String generatedNodeId;
    Taxon taxon;
    String deltaStatus;
    NodeData nodeData;

    public NodeData getNodeData() {
        return nodeData;
    }

    public void setNodeData(NodeData nodeData) {
        this.nodeData = nodeData;
    }
    public String getDeltaStatus() {
        return deltaStatus;
    }

    public void setDeltaStatus(String deltaStatus) {
        this.deltaStatus = deltaStatus;
    }

    public int getResourceId() {
        return resourceId;
    }

    public void setResourceId(int resourceId) {
        this.resourceId = resourceId;
    }

    public ArrayList<Association> getAssociations() {
        return associations;
    }

    public Taxon getTaxon() {
        return taxon;
    }

    public void setTaxon(Taxon taxon) {
        this.taxon = taxon;
    }

    public void setAssociations(ArrayList<Association> associations) {
        this.associations = associations;
    }

    public ArrayList<MeasurementOrFact> getMeasurementOrFacts() {
        return measurementOrFacts;
    }

    public void setMeasurementOrFacts(ArrayList<MeasurementOrFact> measurementOrFacts) {
        this.measurementOrFacts = measurementOrFacts;
    }

    public String getGeneratedNodeId() {
        return generatedNodeId;
    }

    public void setGeneratedNodeId(String generatedNodeId) {
        this.generatedNodeId = generatedNodeId;
    }


    public ArrayList<Media> getMedia() {
        return media;
    }

    public void setMedia(ArrayList<Media> media) {
        this.media = media;
    }

    public ArrayList<Occurrence> getOccurrences() {
        return occurrences;
    }

    public void setOccurrences(ArrayList<Occurrence> occurrences) {
        this.occurrences = occurrences;
    }

    public ArrayList<Reference> getReferences() {
        return references;
    }

    public void setReferences(ArrayList<Reference> references) {
        this.references = references;
    }

    public ArrayList<VernacularName> getVernaculars() {
        return vernaculars;
    }

    public void setVernaculars(ArrayList<VernacularName> vernaculars) {
        this.vernaculars = vernaculars;
    }
}
