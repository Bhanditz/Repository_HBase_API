package org.bibalex.org.hbase.api;

import org.bibalex.org.hbase.models.NodeRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.MediaType;
import org.bibalex.org.hbase.handler.*;
import java.util.List;

@RestController
@RequestMapping("/api")
public class HbaseController {
	private static final Logger logger = LoggerFactory.getLogger(HbaseController.class);

	@RequestMapping(value = "/addHEntry", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
    public ResponseEntity<HbaseResult> addHierarchyEntry(@RequestBody NodeRecord hE) {
		logger.debug("addHEntry");


		HbaseHandler hb = HbaseHandler.getHbaseHandler();
		NodesHandler nodesHandler = new NodesHandler(hb, "Nodes", "nodes_cf.properties");
		HbaseResult result = new HbaseResult();
		result.setStatus(nodesHandler.addNode(hE));

//		hE.setScientificName(hE.getScientificName() + "---1");
		HttpHeaders headers = new HttpHeaders();
        headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
        headers.add("Pragma", "no-cache");
        headers.add("Expires", "0");   
        
		return ResponseEntity
                .ok()
                .headers(headers)
                .contentType(
                        MediaType.parseMediaType("application/json"))
                .body(result);
	}

	@RequestMapping(value = "/updateHEntry", method = RequestMethod.POST, consumes = "application/json", produces = "application/json")
	public ResponseEntity<HbaseResult> updateHierarchyEntry(@RequestBody NodeRecord he) {
		logger.debug("addHEntry");


		HbaseHandler hb = HbaseHandler.getHbaseHandler();
		NodesHandler nodesHandler = new NodesHandler(hb, "Nodes", "nodes_cf.properties");
		HbaseResult result = new HbaseResult();
		result.setStatus(nodesHandler.updateRow(he));

//		hE.setScientificName(hE.getScientificName() + "---1");
		HttpHeaders headers = new HttpHeaders();
		headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
		headers.add("Pragma", "no-cache");
		headers.add("Expires", "0");

		return ResponseEntity
				.ok()
				.headers(headers)
				.contentType(
						MediaType.parseMediaType("application/json"))
				.body(result);
	}

	@RequestMapping(value = "/getLatestNodesOfResource/{resourceId}", method = RequestMethod.GET, produces = "application/json")
	public ResponseEntity<List<NodeRecord>> getLatestNodesOfResource(@PathVariable("resourceId") int resourceId) {

		HbaseHandler hb = HbaseHandler.getHbaseHandler();
		NodesHandler nodesHandler = new NodesHandler(hb, "Nodes", "nodes_cf.properties");

		List<NodeRecord> list = nodesHandler.getNodesOfResource(resourceId, null, null);
//		hE.setScientificName(hE.getScientificName() + "---1");
		HttpHeaders headers = new HttpHeaders();
		headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
		headers.add("Pragma", "no-cache");
		headers.add("Expires", "0");

		return ResponseEntity
				.ok()
				.headers(headers)
				.contentType(
						MediaType.parseMediaType("application/json"))
				.body(list);
	}

	@RequestMapping(value = "/getLatestUpdates/{lastHarvestedTime}", method = RequestMethod.GET, produces = "application/json")
	public ResponseEntity<List<NodeRecord>> getLatestUpdates(@PathVariable("lastHarvestedTime") String lastHarvestedTime) {

		HbaseHandler hb = HbaseHandler.getHbaseHandler();
		NodesHandler nodesHandler = new NodesHandler(hb, "Nodes", "nodes_cf.properties");

		List<NodeRecord> list = nodesHandler.getNodesOfResource(-1, lastHarvestedTime, null);
		HttpHeaders headers = new HttpHeaders();
		headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
		headers.add("Pragma", "no-cache");
		headers.add("Expires", "0");

		return ResponseEntity
				.ok()
				.headers(headers)
				.contentType(
						MediaType.parseMediaType("application/json"))
				.body(list);
	}

	@RequestMapping(value = "/deleteNode/{resourceId}/{generatedNodeId}", method = RequestMethod.GET, produces = "application/json")
	public ResponseEntity<HbaseResult> deleteNode(@PathVariable("resourceId") int resourceId,
												  @PathVariable("generatedNodeId") int generatedNodeId) {

		logger.debug("deleteNode");


		HbaseHandler hb = HbaseHandler.getHbaseHandler();
		NodesHandler nodesHandler = new NodesHandler(hb, "Nodes", "nodes_cf.properties");
		HbaseResult result = new HbaseResult();
		result.setStatus(nodesHandler.deleteNode(resourceId,generatedNodeId));

		HttpHeaders headers = new HttpHeaders();
		headers.add("Cache-Control", "no-cache, no-store, must-revalidate");
		headers.add("Pragma", "no-cache");
		headers.add("Expires", "0");

		return ResponseEntity
				.ok()
				.headers(headers)
				.contentType(
						MediaType.parseMediaType("application/json"))
				.body(result);
	}

	@RequestMapping(value = "/test", method = RequestMethod.GET)
	public ResponseEntity<String> downloadResource() {
		System.out.println("----------------------------x----------------------------");
		return new ResponseEntity(HttpStatus.OK);
	}
}
