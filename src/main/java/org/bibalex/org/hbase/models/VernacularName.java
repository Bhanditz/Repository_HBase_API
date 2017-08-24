package org.bibalex.org.hbase.models;

import java.io.Serializable;

public class VernacularName implements Serializable {
    String name; // used as hbase key of a vernacular name
    String source;
    String language;
    String locality;
    String countryCode;
    //String as it will be saved to Hbase as string and it is read from file as string. No need to save it as boolean
    String isPreferred;
    String taxonRemarks;


    public void setName(String name) {

        this.name = name;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public void setLocality(String locality) {
        this.locality = locality;
    }

    public void setCountryCode(String countryCode) {
        this.countryCode = countryCode;
    }

    public void setIsPreferred(String isPreferred) {
        this.isPreferred = isPreferred;
    }

    public void setTaxonRemarks(String taxonRemarks) {
        this.taxonRemarks = taxonRemarks;
    }

    public String getName() {

        return name;
    }

    public String getSource() {
        return source;
    }

    public String getLanguage() {
        return language;
    }

    public String getLocality() {
        return locality;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public String getIsPreferred() {
        return isPreferred;
    }

    public String getTaxonRemarks() {
        return taxonRemarks;
    }
}
