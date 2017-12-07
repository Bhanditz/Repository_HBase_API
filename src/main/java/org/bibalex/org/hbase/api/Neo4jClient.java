package org.bibalex.org.hbase.api;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.bibalex.org.hbase.PropertiesHandler;
import org.bibalex.org.hbase.models.NodeData;
import org.springframework.web.client.RestTemplate;

public class Neo4jClient {

    public static NodeData getNodeData(String generatedNodeId)
    {
//        final String uri = PropertiesHandler.getProperty("neo4j.api.address") +
//                PropertiesHandler.getProperty("get.node.data.action");

        final String uri = "";

        Map<String, String> params = new HashMap<String, String>();
        params.put("generatedNodeId", generatedNodeId);

        RestTemplate restTemplate = new RestTemplate();
        NodeData result = restTemplate.getForObject(uri, NodeData.class, params);

        return result;
    }

    public static void main(String[] args) throws IOException {
        PropertiesHandler.initializeProperties();
        NodeData v = Neo4jClient.getNodeData("177");
        System.out.println(v.getChildren() == null);

    }
}
