package org.apache.solr.cloud;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Replica;
import org.apache.solr.common.util.TimeSource;
import org.apache.solr.util.TimeOut;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestAbdicateLeadership extends SolrCloudTestCase {

    private static final String COLLECTION_NAME = "collection1";

    @BeforeClass
    public static void setupCluster() throws Exception {

        configureCluster(3)
                .addConfig(COLLECTION_NAME, configset("cloud-minimal"))
                .configure();

        CollectionAdminResponse resp = CollectionAdminRequest.createCollection(COLLECTION_NAME, COLLECTION_NAME, 1, 3) .process(cluster.getSolrClient());
        assertEquals("Admin request failed; ", 0, resp.getStatus());
        cluster.waitForActiveCollection(COLLECTION_NAME, 1, 3);
    }

    @Test
    public void test() throws IOException, SolrServerException, TimeoutException, InterruptedException {
        DocCollection dc = cluster.getSolrClient().getZkStateReader()
                .getClusterState().getCollection(COLLECTION_NAME);
        Replica leaderReplica = dc.getSlice("shard1").getLeader();
        JettySolrRunner runner = cluster.getReplicaJetty(leaderReplica);
        AtomicBoolean closed = new AtomicBoolean(false);
        Thread thread = new Thread(() -> {
            int i = 0;
            while (!closed.get()) {
                try {
                    cluster.getSolrClient().add(COLLECTION_NAME, new SolrInputDocument("id", String.valueOf(i++)));
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) { e.printStackTrace(); }
            }
        });
        thread.start();
        try (SolrClient solrClient = getHttpSolrClient(runner.getBaseUrl().toString())){
            CoreAdminResponse response = CoreAdminRequest.abdicateLeadership(solrClient);
        }
        TimeOut timeOut = new TimeOut(10, TimeUnit.SECONDS, TimeSource.CURRENT_TIME);
        timeOut.waitFor("Timeout waiting for a new leader", () -> {
            ClusterState state = cluster.getSolrClient().getZkStateReader().getClusterState();
            Replica newLeader = state.getCollection(COLLECTION_NAME).getSlice("shard1").getLeader();
            return newLeader != null && !newLeader.getNodeName().equals(runner.getNodeName());
        });
        closed.set(true);
        thread.join();
    }

}
