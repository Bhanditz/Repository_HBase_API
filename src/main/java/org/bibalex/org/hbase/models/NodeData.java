package org.bibalex.org.hbase.models;
import java.io.Serializable;
import java.util.ArrayList;

public class NodeData
{
    ArrayList<String> ancestors = new ArrayList<>();
    ArrayList<String> children = new ArrayList<>();
    ArrayList<String> synonyms = new ArrayList<>();

    public ArrayList<String> getAncestors() {
        return ancestors;
    }

    public ArrayList<String> getChildren() {
        return children;
    }

    public ArrayList<String> getSynonyms() {
        return synonyms;
    }

    public void setData(ArrayList<String> ancestors, ArrayList<String> children, ArrayList<String> synonyms) {
        this.ancestors = ancestors;
        this.children = children;
        this.synonyms = synonyms;
    }
}