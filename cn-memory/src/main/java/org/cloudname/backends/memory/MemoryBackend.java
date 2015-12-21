package org.cloudname.backends.memory;

import org.cloudname.core.AvailabilityListener;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Memory backend. This is the canonical implementation. The synchronization is probably not
 * optimal but for testing this is OK. It defines the correct behaviour for backends, including
 * calling listeners, return values and uniqueness. The actual timing of the various backends
 * will of course vary.
 *
 * @author stalehd@gmail.com
 */
public class MemoryBackend implements CloudnameBackend {
    private final Map<CloudnamePath,String> temporaryLeases = new HashMap<>();
    private final Map<CloudnamePath,String> permanentLeases = new HashMap<>();
    private final Map<CloudnamePath, Set<LeaseListener>> observedTemporaryPaths = new HashMap<>();
    private final Map<CloudnamePath, Set<LeaseListener>> observedPermanentPaths = new HashMap<>();
    private final Object syncObject = new Object();
    private final Random random = new Random();
    private final AtomicBoolean availableFlag = new AtomicBoolean(true);
    private final Set<AvailabilityListener> availabilityListeners = new HashSet<>();


    /* package-private */ void removeTemporaryLease(final CloudnamePath leasePath) {
        synchronized (syncObject) {
            if (temporaryLeases.containsKey(leasePath)) {
                temporaryLeases.remove(leasePath);
                notifyTempObservers(leasePath, (listener) -> listener.leaseRemoved(leasePath));
            }
        }
    }

    private String createRandomInstanceName() {
        return Long.toHexString(random.nextLong());
    }

    /**
     * Notfy observers of temporary paths if applicable.
     */
    private void notifyTempObservers(
            final CloudnamePath path, final Consumer<LeaseListener> method) {
        observedTemporaryPaths.keySet()
                .stream()
        .filter((observedPath) -> observedPath.isSubpathOf(path))
        .forEach((observedPath)
                -> observedTemporaryPaths.get(observedPath).forEach(method::accept));
    }

    /**
     * Notify observers of permanent paths if applicable.
     */
    private void notifyPermObservers(
            final CloudnamePath path, final Consumer<LeaseListener> method) {

        observedPermanentPaths.keySet()
                .stream()
                .filter((observedPath) -> observedPath.isSubpathOf(path))
                .forEach((observedPath)
                        -> observedPermanentPaths.get(observedPath).forEach(method::accept));
    }

    @Override
    public boolean createPermanantLease(final CloudnamePath path, final String data) {
        assert path != null : "Path to lease must be set!";
        assert data != null : "Lease data is required";

        if (!availableFlag.get()) {
            return false;
        }

        synchronized (syncObject) {
            if (permanentLeases.containsKey(path)) {
                return false;
            }
            permanentLeases.put(path, data);
            notifyPermObservers(path, (listener) -> listener.leaseCreated(path, data));
        }
        return true;
    }

    @Override
    public boolean removePermanentLease(final CloudnamePath path) {
        synchronized (syncObject) {
            if (!permanentLeases.containsKey(path)) {
                return false;
            }
            permanentLeases.remove(path);
            notifyPermObservers(path, (listener) -> listener.leaseRemoved(path));
        }
        return true;
    }

    @Override
    public boolean writePermanentLeaseData(final CloudnamePath path, final String data) {
        if (!availableFlag.get()) {
            return false;
        }
        synchronized (syncObject) {
            if (!permanentLeases.containsKey(path)) {
                return false;
            }
            permanentLeases.put(path, data);
            notifyPermObservers(path, (listener) -> listener.dataChanged(path, data));
        }
        return true;
    }

    @Override
    public String readPermanentLeaseData(final CloudnamePath path) {
        if (!availableFlag.get()) {
            return null;
        }
        synchronized (syncObject) {
            if (!permanentLeases.containsKey(path)) {
                return null;
            }
            return permanentLeases.get(path);
        }
    }

    @Override
    public boolean writeTemporaryLeaseData(final CloudnamePath path, final String data) {
        synchronized (syncObject) {
            if (!temporaryLeases.containsKey(path)) {
                return false;
            }
            temporaryLeases.put(path, data);
            notifyTempObservers(path, (listener) -> listener.dataChanged(path, data));
        }
        return true;
    }

    @Override
    public String readTemporaryLeaseData(final CloudnamePath path) {
        synchronized (syncObject) {
            if (!temporaryLeases.containsKey(path)) {
                return null;
            }
            return temporaryLeases.get(path);
        }
    }

    @Override
    public LeaseHandle createTemporaryLease(final CloudnamePath path, final String data) {
        synchronized (syncObject) {
            final String instanceName = createRandomInstanceName();
            CloudnamePath instancePath = new CloudnamePath(path, instanceName);
            while (temporaryLeases.containsKey(instancePath)) {
                instancePath = new CloudnamePath(path, instanceName);
            }
            temporaryLeases.put(instancePath, data);
            final CloudnamePath thePath = instancePath;
            notifyTempObservers(instancePath, (listener) -> listener.leaseCreated(thePath, data));
            return new MemoryLeaseHandle(this, instancePath, availableFlag);
        }
    }

    /**
     * Generate created events for temporary leases for newly attached listeners.
     */
    private void regenerateEventsForTemporaryListener(
            final CloudnamePath path, final LeaseListener listener) {
        temporaryLeases.keySet().stream()
                .filter(path::isSubpathOf)
                .forEach((temporaryPath) ->
                        listener.leaseCreated(temporaryPath, temporaryLeases.get(temporaryPath)));
    }

    /**
     * Generate created events on permanent leases for newly attached listeners.
     */
    private void regenerateEventsForPermanentListener(
            final CloudnamePath path, final LeaseListener listener) {

        permanentLeases.keySet().stream()
                .filter(path::isSubpathOf)
                .forEach((permanentPath) ->
                        listener.leaseCreated(permanentPath, permanentLeases.get(permanentPath)));
    }

    @Override
    public void addTemporaryLeaseListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        synchronized (syncObject) {
            Set<LeaseListener> listeners = observedTemporaryPaths.get(pathToObserve);
            if (listeners == null) {
                listeners = new HashSet<>();
            }
            listeners.add(listener);
            observedTemporaryPaths.put(pathToObserve, listeners);
            regenerateEventsForTemporaryListener(pathToObserve, listener);
        }
    }

    @Override
    public void removeTemporaryLeaseListener(final LeaseListener listener) {
        synchronized (syncObject) {
            observedTemporaryPaths.values()
                    .stream()
                    .filter((listeners) -> listeners.contains(listener))
                    .forEach((listeners) -> listeners.remove(listener));
        }
    }

    @Override
    public void addPermanentLeaseListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        synchronized (syncObject) {
            final Set<LeaseListener> listeners = observedPermanentPaths.getOrDefault(
                    pathToObserve, new HashSet<>());
            listeners.add(listener);
            observedPermanentPaths.put(pathToObserve, listeners);
            regenerateEventsForPermanentListener(pathToObserve, listener);
        }
    }

    @Override
    public void removePermanentLeaseListener(final LeaseListener listener) {
        synchronized (syncObject) {
            observedPermanentPaths.values()
                    .stream()
                    .filter((listeners) -> listeners.contains(listener))
                    .forEach((listeners) -> listeners.remove(listener));
        }
    }

    @Override
    public void close() {
        synchronized (syncObject) {
            observedTemporaryPaths.clear();
            observedPermanentPaths.clear();
            availableFlag.set(true);
        }
    }

    @Override
    public void addAvailableListener(final AvailabilityListener listener) {
        availabilityListeners.add(listener);
    }

    private void setBackendUnavailable() {
        synchronized (syncObject) {
            observedPermanentPaths.values().forEach(leaseListeners
                    -> leaseListeners.forEach(LeaseListener::listenerClosed));
            observedPermanentPaths.clear();
            observedTemporaryPaths.values().forEach(leaseListeners
                    -> leaseListeners.forEach(LeaseListener::listenerClosed));
            observedTemporaryPaths.clear();
            availabilityListeners.forEach((listener)
                    -> listener.availabilityChange(AvailabilityListener.State.UNAVAILABLE));

        }
    }

    private void setBackendAvailable() {
        synchronized (syncObject) {
            availabilityListeners.forEach((listener)
                    -> listener.availabilityChange(AvailabilityListener.State.AVAILABLE));
        }
    }

    /* package-private */ void setAvailable(final boolean available) {
        if (availableFlag.compareAndSet(!available, available)) {
            // State change - if unavailable invalidate all leases, remove temporary leases and
            // make the class immutable.
            if (!available) {
                setBackendUnavailable();
            } else {
                setBackendAvailable();
            }
        }
    }
}
