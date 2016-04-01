package org.cloudname.backends.zookeeper;

import org.apache.curator.test.TestingCluster;
import org.cloudname.core.BackendManager;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.testtools.backend.CoreBackendTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Test the ZooKeeper backend.
 */
public class ZooKeeperBackendTest extends CoreBackendTest {
    private static TestingCluster testCluster;
    private AtomicReference<CloudnameBackend> backend = new AtomicReference<>(null);

    @BeforeClass
    public static void setUp() throws Exception {
        testCluster = new TestingCluster(3);
        testCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCluster.stop();
    }

    @Override
    protected CloudnameBackend getBackend() {
        if (backend.get() == null) {
            backend.compareAndSet(null,
                    BackendManager.getBackend("zookeeper://" + testCluster.getConnectString()));
        }
        return backend.get();

    }

    @Override
    protected void setBackendAvailable() {
        try {
            testCluster.start();
            Thread.sleep(3000);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    protected void setBackendUnavailable() {
        try {
            testCluster.stop();
            Thread.sleep(1000);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
