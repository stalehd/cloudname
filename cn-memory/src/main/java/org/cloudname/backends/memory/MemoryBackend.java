package org.cloudname.backends.memory;

import org.cloudname.core.BackendListener;
import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;
import org.cloudname.core.LeaseType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Memory backend. This is the canonical implementation. The synchronization is probably not
 * optimal but for testing this is OK. It defines the correct behaviour for backends, including
 * calling listeners, return values and uniqueness. The actual timing of the various backends
 * will of course vary.
 *
 * @author stalehd@gmail.com
 */
public class MemoryBackend implements CloudnameBackend {
    private enum LeaseEvent {
        CREATED,
        REMOVED,
        DATA
    }

    private final Map<CloudnamePath, String> leases = new HashMap<>();

    private final Map<CloudnamePath, Set<LeaseListener>> observedPaths = new HashMap<>();
    private final Object syncObject = new Object();
    private final List<BackendListener> backendListeners = new ArrayList<>();
    private final AtomicBoolean enabled = new AtomicBoolean(true);

    /**
     * Notify observers of changes.
     */
    private void notifyObservers(
            final CloudnamePath path, final LeaseEvent event, final String data) {
        observedPaths.keySet().forEach((observedPath) -> {
            if (observedPath.isSubpathOf(path)) {
                // The path matches; notify listeners
                observedPaths.get(observedPath).forEach((listener) -> {
                    switch (event) {
                        case CREATED:
                            listener.leaseCreated(path, data);
                            break;
                        case REMOVED:
                            listener.leaseRemoved(path);
                            break;
                        case DATA:
                            listener.dataChanged(path, data);
                            break;
                        default:
                            throw new RuntimeException("Don't know how to handle " + event);
                    }
                });
            }
        });
    }

    @Override
    public LeaseHandle createLease(
            final LeaseType type, final CloudnamePath path, final String data) {
        if (type == null) {
            return null;
        }
        if (path == null) {
            return null;
        }
        if (data == null) {
            return null;
        }
        if (!enabled.get()) {
            return null;
        }

        synchronized (syncObject) {
            if (leases.containsKey(path)) {
                return null;
            }
            leases.put(path, data);
            notifyObservers(path, LeaseEvent.CREATED, data);
        }
        return new MemoryLeaseHandle(this, path);
    }

    @Override
    public boolean removeLease(final CloudnamePath path) {
        if (!enabled.get()) {
            return false;
        }
        synchronized (syncObject) {
            if (!leases.containsKey(path)) {
                return false;
            }
            leases.remove(path);
            notifyObservers(path, LeaseEvent.REMOVED, null);
        }
        return true;
    }

    @Override
    public boolean writeLeaseData(final CloudnamePath path, final String data) {
        if (!enabled.get()) {
            return false;
        }
        synchronized (syncObject) {
            if (!leases.containsKey(path)) {
                return false;
            }
            leases.put(path, data);
            notifyObservers(path, LeaseEvent.DATA, data);
        }
        return true;
    }

    @Override
    public String readLeaseData(final CloudnamePath path) {
        if (!enabled.get()) {
            return null;
        }
        synchronized (syncObject) {
            if (!leases.containsKey(path)) {
                return null;
            }
            return leases.get(path);
        }
    }

    /**
     * Generate created events for temporary leases for newly attached listeners.
     */
    private void regenerateEventsForListeners(
            final CloudnamePath path, final LeaseListener listener) {
        leases.keySet().forEach((temporaryPath) -> {
           if (path.isSubpathOf(temporaryPath)) {
               listener.leaseCreated(temporaryPath, leases.get(temporaryPath));
           }
       });
    }

    @Override
    public void addLeaseListener(final CloudnamePath leaseToObserve, final LeaseListener listener) {
        if (!enabled.get()) {
            return;
        }
        synchronized (syncObject) {
            final Set<LeaseListener> listeners
                    = observedPaths.getOrDefault(leaseToObserve, new HashSet<>());
            listeners.add(listener);
            observedPaths.put(leaseToObserve, listeners);
            regenerateEventsForListeners(leaseToObserve, listener);
        }
    }

    @Override
    public void addLeaseCollectionListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        if (!enabled.get()) {
            return;
        }
        synchronized (syncObject) {
            final Set<LeaseListener> listeners
                    = observedPaths.getOrDefault(pathToObserve, new HashSet<>());
            listeners.add(listener);
            observedPaths.put(pathToObserve, listeners);
            regenerateEventsForListeners(pathToObserve, listener);
        }
    }

    @Override
    public void removeLeaseListener(final LeaseListener listener) {
        if (!enabled.get()) {
            return;
        }
        synchronized (syncObject) {
            for (final Set<LeaseListener> listeners : observedPaths.values()) {
                if (listeners.contains(listener)) {
                    listeners.remove(listener);
                    return;
                }
            }
        }
    }

    @Override
    public void close() {
        synchronized (syncObject) {
            observedPaths.clear();
        }
    }

    @Override
    public void addBackendListener(final BackendListener listener) {
        synchronized (syncObject) {
            backendListeners.add(listener);
        }
    }

    /**
     * Enable the backend; emulate backend coming back up.
     */
    public void enable() {
        enabled.set(true);
        synchronized (syncObject) {
            backendListeners.forEach(BackendListener::backendIsAvailable);
        }
    }

    /**
     * Disable the backend; emulate network partition or backend failing.
     */
    public void disable() {
        synchronized (syncObject) {
            backendListeners.forEach(BackendListener::backendIsUnavailable);

            observedPaths.forEach((path, listeners) -> {
                listeners.forEach((listener) -> {
                    listener.leaseRemoved(path);
                });
            });
            observedPaths.clear();
            leases.clear();
        }
        enabled.set(false);
    }
}
