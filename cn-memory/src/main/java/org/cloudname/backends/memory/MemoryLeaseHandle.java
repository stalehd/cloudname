package org.cloudname.backends.memory;

import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A handle returned to clients acquiring temporary leases.
 *
 * @author stalehd@gmail.com
 */
public class MemoryLeaseHandle implements LeaseHandle {
    private final MemoryBackend backend;
    private final CloudnamePath clientLeasePath;
    private final AtomicBoolean expired = new AtomicBoolean(false);
    private final AtomicBoolean availableFlag;
    /**
     * New lease instance.
     *
     * @param backend The backend issuing the lease
     * @param clientLeasePath The path to the lease
     */
    public MemoryLeaseHandle(
            final MemoryBackend backend, final CloudnamePath clientLeasePath,
            final AtomicBoolean availableFlag) {
        this.backend = backend;
        this.clientLeasePath = clientLeasePath;
        expired.set(false);
        this.availableFlag = availableFlag;
    }

    @Override
    public boolean writeLeaseData(final String data) {
        if (!availableFlag.get()) {
            return false;
        }
        return backend.writeTemporaryLeaseData(clientLeasePath, data);
    }

    @Override
    public CloudnamePath getLeasePath() {
        if (expired.get()) {
            return null;
        }
        if (!availableFlag.get()) {
            return null;
        }
        return clientLeasePath;
    }

    @Override
    public void close() throws IOException {
        if (!availableFlag.get()) {
            return;
        }
        backend.removeTemporaryLease(clientLeasePath);
        expired.set(true);
    }
}
