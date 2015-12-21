package org.cloudname.backends.memory;

import org.cloudname.core.BackendManager;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;
import org.cloudname.testtools.backend.CoreBackendTest;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;

/**
 * Test the memory backend. Since the memory backend is the reference implementation this test
 * shouldn't fail. Ever.
 */
public class MemoryBackendTest extends CoreBackendTest {
    private final CloudnameBackend BACKEND = BackendManager.getBackend("memory://");
    @Override
    protected CloudnameBackend getBackend() {
        assertThat("Expected backend to be registered", BACKEND, is(notNullValue()));
        return BACKEND;
    }

    @Test
    public void ensureUnavailableAvailableBackendNotifiesListener() throws Exception {
        try (final CloudnameBackend backend = getBackend()) {

            final CountDownLatch availableLatch = new CountDownLatch(1);
            final CountDownLatch unavailableLatch = new CountDownLatch(1);

            backend.addAvailableListener((newState) -> {
                switch (newState) {
                    case AVAILABLE:
                        availableLatch.countDown();
                        break;
                    case UNAVAILABLE:
                        unavailableLatch.countDown();
                        break;
                    default:
                        break;
                }
            });

            setUnavailble((MemoryBackend) backend);

            assertThat(unavailableLatch.await(1000, TimeUnit.MILLISECONDS), is(true));

            setAvailable((MemoryBackend) backend);

            assertThat(availableLatch.await(1000, TimeUnit.MILLISECONDS), is(true));
        }
    }

    @Test
    public void ensureUnavailableNotifiesLeases() throws Exception {
        try (final CloudnameBackend backend = getBackend()) {

            final CloudnamePath createdPath = new CloudnamePath(new String[]{"existing"});
            final CloudnamePath removedPath = new CloudnamePath(new String[]{"removed"});
            final CloudnamePath nadaPath = new CloudnamePath(new String[]{"nothing"});

            final CountDownLatch closeLatch = new CountDownLatch(4);
            final CountDownLatch createLatch = new CountDownLatch(2);
            final CountDownLatch removeLatch = new CountDownLatch(1);

            // Add three listeners: One pointing to an existing leases, one to a removed lease
            // and one pointing to a lease that has never existed and a permanent lease.
            backend.addTemporaryLeaseListener(createdPath,
                    new LeaseListener() {
                        @Override
                        public void leaseCreated(CloudnamePath path, String data) {
                            createLatch.countDown();
                        }

                        @Override
                        public void leaseRemoved(CloudnamePath path) {

                        }

                        @Override
                        public void dataChanged(CloudnamePath path, String data) {

                        }

                        @Override
                        public void listenerClosed() {
                            closeLatch.countDown();
                        }
                    });

            backend.addTemporaryLeaseListener(removedPath,
                    new LeaseListener() {
                        @Override
                        public void leaseCreated(CloudnamePath path, String data) {
                            createLatch.countDown();
                        }

                        @Override
                        public void leaseRemoved(CloudnamePath path) {
                            removeLatch.countDown();
                        }

                        @Override
                        public void dataChanged(CloudnamePath path, String data) {

                        }

                        @Override
                        public void listenerClosed() {
                            closeLatch.countDown();
                        }
                    });

            backend.addTemporaryLeaseListener(nadaPath,
                    new LeaseListener() {
                        @Override
                        public void leaseCreated(CloudnamePath path, String data) {

                        }

                        @Override
                        public void leaseRemoved(CloudnamePath path) {

                        }

                        @Override
                        public void dataChanged(CloudnamePath path, String data) {

                        }

                        @Override
                        public void listenerClosed() {
                            closeLatch.countDown();
                        }
                    });

            backend.addPermanentLeaseListener(nadaPath, new LeaseListener() {
                @Override
                public void leaseCreated(CloudnamePath path, String data) {

                }

                @Override
                public void leaseRemoved(CloudnamePath path) {

                }

                @Override
                public void dataChanged(CloudnamePath path, String data) {

                }

                @Override
                public void listenerClosed() {
                    closeLatch.countDown();
                }
            });

            final LeaseHandle handle = backend.createTemporaryLease(removedPath, "Path to be removed");
            handle.close();

            backend.createTemporaryLease(createdPath, "Path to be created");


            assertThat(createLatch.await(getBackendPropagationTime(), TimeUnit.MICROSECONDS),
                    is(true));
            assertThat(removeLatch.await(getBackendPropagationTime(), TimeUnit.MICROSECONDS),
                    is(true));

            setUnavailble((MemoryBackend) backend);

            assertThat(closeLatch.await(getBackendPropagationTime(), TimeUnit.MILLISECONDS),
                    is(true));
        }
    }

    @Test
    public void ensureLeaseHandlesAreImmutableWhenBackendIsUnavailable() throws Exception {
        try (final CloudnameBackend backend = getBackend()) {

            final LeaseHandle handle = backend.createTemporaryLease(
                    new CloudnamePath(new String[] {"foo"}), "some random");
            setUnavailble((MemoryBackend) backend);

            assertThat(handle.getLeasePath(), is(nullValue()));
            assertThat(handle.writeLeaseData("anyData"), is(false));
        }
    }

    @Test
    public void ensurePermanentLeasesAreImmutableWhenBackendIsUnavailable() throws Exception {
        try (final CloudnameBackend backend = getBackend()) {
            assertThat(backend.createPermanantLease(
                    new CloudnamePath(new String[] {"foo", "permanent"}), "something"), is(true));

            setUnavailble((MemoryBackend)backend);

            final CloudnamePath somePath = new CloudnamePath(new String[] {"foo", "bar"});

            assertThat(backend.createPermanantLease(somePath, "something"), is(false));
            assertThat(backend.writePermanentLeaseData(somePath, "something"), is(false));
            assertThat(backend.removePermanentLease(somePath), is(false));
        }
    }

    private void setAvailable(final MemoryBackend backend) {
        backend.setAvailable(true);
    }

    private void setUnavailble(final MemoryBackend backend) {
        backend.setAvailable(false);
    }
}
