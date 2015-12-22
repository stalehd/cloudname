package org.cloudname.backends.zookeeper;

import org.apache.curator.test.TestingCluster;
import org.cloudname.core.BackendManager;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.testtools.backend.CoreBackendTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Test the ZooKeeper backend.
 */
public class ZooKeeperBackendTest extends CoreBackendTest {
    private static TestingCluster testCluster;
    private AtomicReference<CloudnameBackend> backend = new AtomicReference<>(null);
    private AtomicBoolean unavailable = new AtomicBoolean(false);

    @BeforeClass
    public static void setUp() throws Exception {
        testCluster = new TestingCluster(3);
        testCluster.start();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testCluster.stop();
    }

    protected CloudnameBackend getBackend() {
        if (backend.get() == null) {
            backend.compareAndSet(null,
                    BackendManager.getBackend("zookeeper://" + testCluster.getConnectString()));
        }
        if (unavailable.get()) {
            setAvailable(backend.get());
        }
        return backend.get();

    }

    @Override
    protected boolean canSetAvailability() {
        return true;
    }

    @Override
    protected void setAvailable(final CloudnameBackend backend) {
        // Cheat a bit by just triggering the unavailable state internally. Restarting the ZooKeeper
        // test cluster is horribly slow and error prone.
        if (backend instanceof ZooKeeperBackend) {
            ((ZooKeeperBackend)backend).setAvailable();
            unavailable.set(false);
        }
    }

    @Override
    protected void setUnavailable(final CloudnameBackend backend) {
        if (backend instanceof ZooKeeperBackend) {
            ((ZooKeeperBackend)backend).setUnavailable();
            unavailable.set(true);
        }
    }
}
