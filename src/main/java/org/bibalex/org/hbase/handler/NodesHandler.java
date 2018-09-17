package org.bibalex.org.hbase.handler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.bibalex.org.hbase.api.Neo4jClient;
import org.bibalex.org.hbase.models.*;

public class NodesHandler {

    private String tableName;
    private HbaseHandler hbaseHandler;

    private static final String DELETE = "D";

    public NodesHandler(HbaseHandler hbaseHandler, String tableName, String columnFamiliesFilePath)
    {
        this.hbaseHandler = hbaseHandler;
        this.tableName = tableName;
    }

    private UUID generateMediaGUID()
    {
        return UUID.randomUUID();
    }

    public boolean addNode(NodeRecord node)
    {
        String keyParts = node.getResourceId() +  "_" + node.getGeneratedNodeId();

        Put p = new Put(Bytes.toBytes(keyParts));

        // add Vernacular names of node
        if(node.getVernaculars() != null) {
            byte[] namesColumnFamily = Bytes.toBytes("Names");
            for (VernacularName vn : node.getVernaculars()) {
                String columnQualifier = vn.getName() + "_" + vn.getLanguage();
                try {
                    byte[] value = NodesHandler.serialize(vn);
                    p.addColumn(namesColumnFamily, Bytes.toBytes(columnQualifier), value);
                } catch (IOException e) {
                    System.out.println("Vernacular Name serialization error");
                    e.printStackTrace();
                }
            }
        }
        // add References of node
        if(node.getReferences() != null) {
            byte[] referencesColumnFamily = Bytes.toBytes("References");
            for (Reference reference : node.getReferences()) {
                String columnQualifier = reference.getReferenceId();
                try {
                    byte[] value = NodesHandler.serialize(reference);
                    p.addColumn(referencesColumnFamily, Bytes.toBytes(columnQualifier), value);
                } catch (IOException e) {
                    System.out.println("Reference serialization error");
                    e.printStackTrace();
                }
            }
        }

        // handle media lists of taxon
        if(node.getTaxon() == null)
            node.setTaxon(new Taxon());
        if (node.getTaxon().getGuids() == null)
            node.getTaxon().setGuids(new ArrayList<String>());

        if (node.getTaxon().getGuidsMapping() == null)
            node.getTaxon().setGuidsMapping(new HashMap<String,String>());

        // add media
        // add media of node
        byte[] attributesColumnFamily = Bytes.toBytes("Attributes");
        if(node.getMedia() != null) {
            for (Media md : node.getMedia()) {

                md.setGuid(md.getGuid() ==null ? this.generateMediaGUID() + "" : md.getGuid());
                String mediaKeyParts = md.getGuid();
                        Put mediaPut = new Put(Bytes.toBytes(mediaKeyParts));
                String columnQualifier = md.getMediaId() + "_" + md.getType();
                try {
                    byte[] value = NodesHandler.serialize(md);
                    mediaPut.addColumn(attributesColumnFamily, Bytes.toBytes(columnQualifier), value);
                } catch (IOException e) {
                    System.out.println("media serialization error");
                    e.printStackTrace();
                }
                node.getTaxon().getGuids().add(mediaKeyParts);

                // remove duplicates from guids list
                Set<String> hs = new HashSet<String>();
                hs.addAll(node.getTaxon().getGuids());
                node.getTaxon().getGuids().clear();
                node.getTaxon().getGuids().addAll(hs);

                node.getTaxon().getGuidsMapping().put(md.getMediaId(),mediaKeyParts);

                hbaseHandler.addRow("Media", mediaPut);
            }
        }

        // add relations of node
            byte[] relationsColumnFamily = Bytes.toBytes("Relations");
            byte[] columnQualifier = Bytes.toBytes("relation");
            try {
                byte[] value = NodesHandler.serialize(node.getTaxon());
                p.addColumn(relationsColumnFamily, columnQualifier, value);
            } catch (IOException e) {
                System.out.println("Taxon serialization error");
                e.printStackTrace();
            }

        // add traits of node
        byte[] traitsColumnFamily = Bytes.toBytes("Traits");
        // add occurrences
        if(node.getOccurrences() != null) {
            for (Occurrence occurrence : node.getOccurrences()) {
                String occurrenceColumnQualifier = occurrence.getOccurrenceId();
                try {
                    byte[] value = NodesHandler.serialize(occurrence);
                    p.addColumn(traitsColumnFamily, Bytes.toBytes(occurrenceColumnQualifier), value);
                } catch (IOException e) {
                    System.out.println("occurrences serialization error");
                    e.printStackTrace();
                }
            }
        }
        // add measurementOrFacts
        if(node.getMeasurementOrFacts() != null) {
            for (MeasurementOrFact measurementOrFact : node.getMeasurementOrFacts()) {
                String measurementColumnQualifier = measurementOrFact.getOccurrenceId() + "_" +
                        measurementOrFact.getMeasurementType() + "_" +
                        measurementOrFact.getMeasurementValue();
                try {
                    byte[] value = NodesHandler.serialize(measurementOrFact);
                    p.addColumn(traitsColumnFamily, Bytes.toBytes(measurementColumnQualifier), value);
                } catch (IOException e) {
                    System.out.println("measurementOrFacts serialization error");
                    e.printStackTrace();
                }
            }
        }
        // add associations
        if(node.getAssociations() != null) {
            for (Association association : node.getAssociations()) {
                String associationColumnQualifier = association.getOccurrenceId() + "_" +
                        association.getTargetOccurrenceId() + "_" +
                        association.getAssociationType();
                try {
                    byte[] value = NodesHandler.serialize(association);
                    p.addColumn(traitsColumnFamily, Bytes.toBytes(associationColumnQualifier), value);
                } catch (IOException e) {
                    System.out.println("association serialization error");
                    e.printStackTrace();
                }
            }
        }
        if(node.getVernaculars() != null || node.getReferences() != null || node.getTaxon() != null || node.getAssociations() != null
                || node.getOccurrences() != null || node.getMeasurementOrFacts() != null)
            return hbaseHandler.addRow(this.tableName, p);
        else
            return false;
    }

    public NodeRecord prepareNodeForDeletion(int resource, int generatedNodeId)
    {
        NodeRecord nodeRecord = new NodeRecord();

        FilterList allFilters = null;
        if(resource != -1) {
            allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            allFilters.addFilter(new PrefixFilter(Bytes.toBytes(resource + "_" + generatedNodeId)));
        }
        try (ResultScanner results = hbaseHandler.scan(this.tableName, allFilters, null)) {
            if (results != null) {
                Result result = results.next();
                if (result != null) {
                    String[] rowKeyParts = Bytes.toString(result.getRow()).split("_");
                    nodeRecord.setGeneratedNodeId(rowKeyParts[1]);
                    nodeRecord.setResourceId(Integer.parseInt(rowKeyParts[0]));

                    // get Vernaculares of node
                    Set<byte[]> vernacularCoulmnQualifiers = result.getFamilyMap(Bytes.toBytes("Names")).keySet();
                    for (byte[] i : vernacularCoulmnQualifiers) {
                        try {
                            VernacularName vn = (VernacularName) NodesHandler.deserialize(result.getValue(Bytes.toBytes("Names"), i));
                            // update delta status for vernaculars
                            vn.setDeltaStatus(DELETE);
                            if (nodeRecord.getVernaculars() == null) {
                                nodeRecord.setVernaculars(new ArrayList<VernacularName>());
                                nodeRecord.getVernaculars().add(vn);
                            } else
                                nodeRecord.getVernaculars().add(vn);
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                            System.out.println("Vernacular Name serialization Error");
                        }
                    }


                    // get references of node
                    Set<byte[]> referencesCoulmnQualifiers = result.getFamilyMap(Bytes.toBytes("References")).keySet();
                    for (byte[] i : referencesCoulmnQualifiers) {
                        try {
                            Reference reference = (Reference) NodesHandler.deserialize(result.getValue(Bytes.toBytes("References"), i));
                            // update delta status for references
                            reference.setDeltaStatus(DELETE);
                            if (nodeRecord.getReferences() == null) {
                                nodeRecord.setReferences(new ArrayList<Reference>());
                                nodeRecord.getReferences().add(reference);
                            } else
                                nodeRecord.getReferences().add(reference);
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                            System.out.println("refernces serialization Error");
                        }
                    }

                    // get traits of node
                    Set<byte[]> traitsCoulmnQualifiers = result.getFamilyMap(Bytes.toBytes("Traits")).keySet();
                    for (byte[] i : traitsCoulmnQualifiers) {
                        try {
                            Object object = NodesHandler.deserialize(result.getValue(Bytes.toBytes("Traits"), i));

                            if (object instanceof Occurrence) {
                                Occurrence oc = (Occurrence) object;
                                oc.setDeltaStatus(DELETE);
                                if (nodeRecord.getOccurrences() == null) {
                                    nodeRecord.setOccurrences(new ArrayList<Occurrence>());
                                    nodeRecord.getOccurrences().add(oc);
                                } else
                                    nodeRecord.getOccurrences().add(oc);

                            } else if (object instanceof Association) {
                                Association as = (Association) object;
                                if (nodeRecord.getAssociations() == null) {
                                    nodeRecord.setAssociations(new ArrayList<Association>());
                                    nodeRecord.getAssociations().add(as);
                                } else
                                    nodeRecord.getAssociations().add(as);

                            } else {
                                MeasurementOrFact ms = (MeasurementOrFact) object;
                                if (nodeRecord.getMeasurementOrFacts() == null) {
                                    nodeRecord.setMeasurementOrFacts(new ArrayList<MeasurementOrFact>());
                                    nodeRecord.getMeasurementOrFacts().add(ms);
                                } else
                                    nodeRecord.getMeasurementOrFacts().add(ms);
                            }
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                            System.out.println("traits serialization Error");
                        }
                    }

                    // get relations of node
                    Set<byte[]> relationsCoulmnQualifiers = result.getFamilyMap(Bytes.toBytes("Relations")).keySet();
                    for (byte[] i : relationsCoulmnQualifiers) {
                        try {
                            Taxon relation = (Taxon) NodesHandler.deserialize(result.getValue(Bytes.toBytes("Relations"), i));
                            // update delta status for relation
                            relation.setDeltaStatus(DELETE);
                            nodeRecord.setTaxon(relation);
                        } catch (ClassNotFoundException e) {
                            e.printStackTrace();
                            System.out.println("relation serialization Error");
                        }
                    }

                    // get media and agents of node
                    if (nodeRecord != null && nodeRecord.getTaxon() != null && nodeRecord.getTaxon().getGuidsMapping() != null) {
                        ArrayList<Media> mediaList = getMediaOfNode(nodeRecord.getTaxon().getGuids(), null);
                        for (Media md : mediaList) {
                            ArrayList<Agent> agentsList =  md.getAgents();
                            for (Agent agent : agentsList) {
                                agent.setDeltaStatus(DELETE);
                            }
                            md.setDeltaStatus(DELETE);
                        }
                        nodeRecord.setMedia(mediaList);
                    }
            }
        }
            return nodeRecord;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }


    }

    public boolean deleteNode(int resource, int generatedNodeId)
    {
        NodeRecord nodeRecord = prepareNodeForDeletion(resource, generatedNodeId);
        return addNode(nodeRecord);
    }

    public List<NodeRecord> getNodesOfResource(int resource, String timestamp, String generatedNodeId)
    {

        List<NodeRecord> allNodesOfResource = new ArrayList<NodeRecord>();

        FilterList allFilters = null;

        if(generatedNodeId != null) {
            allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            allFilters.addFilter(new PrefixFilter(Bytes.toBytes(resource + "_" + generatedNodeId)));
        }
        else if(resource != -1)
        {
            allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            allFilters.addFilter(new PrefixFilter(Bytes.toBytes(resource + "_")));
        }

        try (ResultScanner results = hbaseHandler.scan(this.tableName, allFilters, timestamp)) {

            for (Result result = results.next(); result != null; result = results.next()) {
                NodeRecord node = new NodeRecord();

                // get generated node id
                String[] rowKeyParts = Bytes.toString(result.getRow()).split("_");
                node.setGeneratedNodeId(rowKeyParts[1]);
                node.setResourceId(Integer.parseInt(rowKeyParts[0]));

                // get Vernaculares of node
                Set<byte[]> vernacularCoulmnQualifiers = result.getFamilyMap(Bytes.toBytes("Names")).keySet();
                System.out.println("vernaculars");
                for(byte[] i : vernacularCoulmnQualifiers)
                {
                    try {
                        VernacularName vn = (VernacularName) NodesHandler.deserialize(result.getValue(Bytes.toBytes("Names"), i));
                        if(node.getVernaculars() == null)
                        {
                            node.setVernaculars(new ArrayList<VernacularName>());
                            node.getVernaculars().add(vn);
                        }
                        else
                            node.getVernaculars().add(vn);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        System.out.println("Vernacular Name serialization Error");
                    }
                }

                // get references of node
                Set<byte[]> referencesCoulmnQualifiers = result.getFamilyMap(Bytes.toBytes("References")).keySet();
                for(byte[] i : referencesCoulmnQualifiers)
                {
                    try {
                        Reference reference = (Reference) NodesHandler.deserialize(result.getValue(Bytes.toBytes("References"), i));
                        if(node.getReferences() == null)
                        {
                            node.setReferences(new ArrayList<Reference>());
                            node.getReferences().add(reference);
                        }
                        else
                            node.getReferences().add(reference);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        System.out.println("refernces serialization Error");
                    }
                }

                // get traits of node
                Set<byte[]> traitsCoulmnQualifiers = result.getFamilyMap(Bytes.toBytes("Traits")).keySet();
                for(byte[] i : traitsCoulmnQualifiers)
                {
                    try {
                        Object object = NodesHandler.deserialize(result.getValue(Bytes.toBytes("Traits"), i));
                        if(object instanceof Occurrence) {
                            if(node.getOccurrences() == null)
                            {
                                node.setOccurrences(new ArrayList<Occurrence>());
                                node.getOccurrences().add((Occurrence) object);
                            }
                            else
                                node.getOccurrences().add((Occurrence) object);
                        }
                        else if(object instanceof Association) {
                            if(node.getAssociations() == null)
                            {
                                node.setAssociations(new ArrayList<Association>());
                                node.getAssociations().add((Association) object);
                            }
                            else
                                node.getAssociations().add((Association) object);
                        }
                        else
                        {
                            if(node.getMeasurementOrFacts() == null)
                            {
                                node.setMeasurementOrFacts(new ArrayList<MeasurementOrFact>());
                                node.getMeasurementOrFacts().add((MeasurementOrFact) object);
                            }
                            else
                                node.getMeasurementOrFacts().add((MeasurementOrFact) object);
                        }
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        System.out.println("refernces serialization Error");
                    }
                }

                // get relations of node
                Set<byte[]> relationsCoulmnQualifiers = result.getFamilyMap(Bytes.toBytes("Relations")).keySet();
                for(byte[] i : relationsCoulmnQualifiers)
                {
                    try {
                        Taxon relation = (Taxon) NodesHandler.deserialize(result.getValue(Bytes.toBytes("Relations"), i));
                        node.setTaxon(relation);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        System.out.println("relation serialization Error");
                    }
                }

                // get media and agents of node
                if(node != null && node.getTaxon() != null && node.getTaxon().getGuids() != null) {
                    node.setMedia(getMediaOfNode(node.getTaxon().getGuids(), timestamp));
                }

//                // get ancestors & children & synonyms : call neo4j
//                NodeData nodeData = Neo4jClient.getNodeData(node.getGeneratedNodeId());
//                node.setNodeData(nodeData);



                allNodesOfResource.add(node);
            }
            return allNodesOfResource;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    public ArrayList<Media> getMediaOfNode(ArrayList<String> guids, String timestamp)
    {
        ArrayList<Media> media = new ArrayList<Media>();
        FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        for (String g : guids) {
            allFilters.addFilter(new PrefixFilter(Bytes.toBytes(g)));
        }
        try (ResultScanner results = hbaseHandler.scan("Media", allFilters, timestamp)) {
            for (Result result = results.next(); result != null; result = results.next()) {
                Set<byte[]> attributesCoulmnQualifiers = result.getFamilyMap(Bytes.toBytes("Attributes")).keySet();
                for(byte[] i : attributesCoulmnQualifiers)
                {

                    try {
                        Media md = (Media) NodesHandler.deserialize(result.getValue(Bytes.toBytes("Attributes"), i));
                        media.add(md);
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                        System.out.println("media deserialization Error");
                    }
                }

            }
            return media;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    public boolean updateRow(NodeRecord node)
    {
        List<NodeRecord> result = getNodesOfResource(node.getResourceId(), null, node.getGeneratedNodeId());
        NodeRecord targetNode = result.get(0);
        node.getTaxon().setGuids(targetNode.getTaxon().getGuids());
        node.getTaxon().setGuidsMapping(targetNode.getTaxon().getGuidsMapping());
        return addNode(node);
    }

    public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream os = new ObjectOutputStream(out);
        os.writeObject(obj);
        return out.toByteArray();
    }
    public static Object deserialize(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        ObjectInputStream is = new ObjectInputStream(in);
        return is.readObject();
    }



    public static void main(String[] args) throws IOException
    {

        HbaseHandler hb = HbaseHandler.getHbaseHandler();
        hb.dropTable("Nodes");
        hb.dropTable("Media");
        hb.createTable("Nodes", new String[] { "Names", "References", "Traits", "Relations" } );
        hb.createTable("Media", new String[] { "Attributes", "Agents" } );
    }


}