package org.cloudname.backends.consul;

import org.cloudname.core.CloudnameBackend;
import org.cloudname.core.CloudnamePath;
import org.cloudname.core.LeaseHandle;
import org.cloudname.core.LeaseListener;
import org.cloudname.service.CloudnameService;
import org.cloudname.service.Endpoint;
import org.cloudname.service.InstanceCoordinate;
import org.cloudname.service.LocalServiceListener;
import org.cloudname.service.ServiceCoordinate;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a basic implementation of a CloudName backend. It uses the KV store for all data since
 * the service concept doesn't fit too well with the lease concept. The KV store in conjunction
 * with sessions fits nicely though. This is currently a proof-of-concept implementation tha haven't
 * been tested extensively.
 *
 * @author stalehd@gmail.com
 */
public class ConsulBackend implements CloudnameBackend {
    final Consul consul;

    private static final int SESSION_TTL = 10;
    private static final int LOCK_DELAY = 0;

    private final Map<CloudnamePath, ConsulSession> sessions = new ConcurrentHashMap<>();
    private final Map<LeaseListener, ConsulWatch> watches = new ConcurrentHashMap<>();
    private static final char SEPARATOR = '/';
    private static final String CN_PREFIX = "cn";
    private static final String EPHEMERAL_PREFIX = "ephemeral";
    private static final String PERMANENT_PREFIX = "permanent";

    /**
     * Convert a cloudname path to a session name.
     */
    private String pathToSession(final CloudnamePath path) {
        return CN_PREFIX + SEPARATOR + path.join(SEPARATOR);
    }

    /**
     * Convert a cloudname path to an ephemeral KV key name.
     */
    private String pathToEphemeralKv(final CloudnamePath path) {
        return CN_PREFIX + SEPARATOR + EPHEMERAL_PREFIX + SEPARATOR + path.join(SEPARATOR);
    }

    /**
     * Convert cloudname path to permanent KV key.
     */
    private String pathToPermanentKv(final CloudnamePath path) {
        return CN_PREFIX + SEPARATOR + PERMANENT_PREFIX + SEPARATOR + path.join(SEPARATOR);
    }

    /**
     * Convert ephemeral or permanent key name into a Cloudname path.
     */
    private CloudnamePath kvNameToCloudnamePath(final String name) {
        final String[] elements = name.split("" + SEPARATOR);
        // The first two elements are prefixes; skip those
        return new CloudnamePath(Arrays.copyOfRange(elements, 2, elements.length));
    }

    /**
     * Create new backend connected to the specified endpoint.
     *
     * @throws IllegalArgumentException  the endpoint doesn't exist
     */
    public ConsulBackend(final String consulEndpoint) {
        consul = new Consul(consulEndpoint);
        if (!consul.isValid()) {
            throw new IllegalArgumentException("Consul endpoint " + consulEndpoint
                    + " isn't a valid endpoint");
        }
    }

    // Use a regular random value. Since this is an instance identifier which is well known
    // there's no need for particular randomness.
    private final Random random = new Random();

    // Since Consul doesn't allow cas=0 and acquire=<session id> at the same time we'll have
    // to keep track of the instance IDs we've created
    private final Set<String> createdIds = new HashSet<>();
    private final Object syncObject = new Object();

    /**
     * Get an instance ID with a random name. Ensures that the random instance id isn't used
     * before by this instance. We'll just have to assume that the locks works without race
     * conditions across the cluster. They probably do.
     */
    private String getRandomInstanceId() {
        synchronized (syncObject) {
            String id = Long.toHexString(random.nextLong());
            while (createdIds.contains(id)) {
                id = Long.toHexString(random.nextLong());
            }
            createdIds.add(id);
            return id;
        }
    }

    @Override
    public LeaseHandle createTemporaryLease(final CloudnamePath path, final String data) {
        // Create session with TTL set to <something> and Behavior=delete. The session isn't
        // used to uniquely identify the client but to create ephemeral values in the KV store.
        final ConsulSession session
                = consul.createSession(pathToSession(path), SESSION_TTL, LOCK_DELAY);
        // Create value in KV and set the session owner. cas = 0 to ensure no duplicates. The KV
        // entry is the canonical lease
        boolean leaseAcquired = false;
        final AtomicReference<CloudnamePath> instancePath = new AtomicReference<>();
        while (!leaseAcquired) {
            instancePath.set(new CloudnamePath(path, getRandomInstanceId()));
            leaseAcquired = consul.writeSessionData(
                    pathToEphemeralKv(instancePath.get()), data, session.getId());
        }

        sessions.put(instancePath.get(), session);
        // Optional: Create service and set the session (so that the service appears in DNS)
        //    health check for service is lookup in KV store. The service entry is FYI only

        return new LeaseHandle() {
            @Override
            public boolean writeLeaseData(final String data) {
                if (session.isClosed()) {
                    return false;
                }
                return consul.writeSessionData(
                        pathToEphemeralKv(instancePath.get()), data, session.getId());
            }

            @Override
            public CloudnamePath getLeasePath() {
                if (session.isClosed()) {
                    return null;
                }
                return instancePath.get();
            }

            @Override
            public void close() throws Exception {
                // This will clear the KV entry
                session.close();
                sessions.remove(instancePath.get());
            }
        };
    }

    @Override
    public boolean writeTemporaryLeaseData(final CloudnamePath path, final String data) {
        final ConsulSession session = sessions.get(path);
        if (session == null) {
            return false;
        }
        return consul.writeSessionData(pathToEphemeralKv(path), data, session.getId());
    }

    @Override
    public String readTemporaryLeaseData(final CloudnamePath path) {
        if (path == null) {
            return null;
        }
        return consul.readData(pathToEphemeralKv(path));
    }

    @Override
    public void addTemporaryLeaseListener(
            final CloudnamePath pathToWatch, final LeaseListener listener) {
        final ConsulWatch watch = consul.createWatch(pathToEphemeralKv(pathToWatch));
        watches.put(listener, watch);
        watch.startWatching(new ConsulWatch.ConsulWatchListener() {
            @Override
            public void created(final String valueName, final String value) {
                listener.leaseCreated(kvNameToCloudnamePath(valueName), value);
            }

            @Override
            public void changed(final String valueName, final String value) {
                listener.dataChanged(kvNameToCloudnamePath(valueName), value);
            }

            @Override
            public void removed(final String valueName) {
                listener.leaseRemoved(kvNameToCloudnamePath(valueName));
            }
        });
    }

    @Override
    public void removeTemporaryLeaseListener(final LeaseListener listener) {
        // Remove watcher
        final ConsulWatch watch = watches.get(listener);
        if (watch != null) {
            watch.stop();
        }
    }

    @Override
    public boolean createPermanantLease(final CloudnamePath path, final String data) {
        return consul.createPermanentData(pathToPermanentKv(path), data);
    }

    @Override
    public boolean removePermanentLease(final CloudnamePath path) {
        final String consulPath = pathToPermanentKv(path);
        if (consul.readData(consulPath) == null) {
            return false;
        }
        return consul.removePermanentData(consulPath);
    }

    @Override
    public boolean writePermanentLeaseData(final CloudnamePath path, final String data) {
        return consul.writePermanentData(pathToPermanentKv(path), data);
    }

    @Override
    public String readPermanentLeaseData(final CloudnamePath path) {
        return consul.readData(pathToPermanentKv(path));
    }

    @Override
    public void addPermanentLeaseListener(
            final CloudnamePath pathToObserve, final LeaseListener listener) {
        final ConsulWatch watch = consul.createWatch(pathToPermanentKv(pathToObserve));
        watches.put(listener, watch);
        watch.startWatching(new ConsulWatch.ConsulWatchListener() {
            @Override
            public void created(final String valueName, final String value) {
                listener.leaseCreated(kvNameToCloudnamePath(valueName), value);
            }

            @Override
            public void changed(final String valueName, final String value) {
                listener.dataChanged(kvNameToCloudnamePath(valueName), value);
            }

            @Override
            public void removed(final String valueName) {
                listener.leaseRemoved(kvNameToCloudnamePath(valueName));
            }
        });
    }

    @Override
    public void removePermanentLeaseListener(final LeaseListener listener) {
        final ConsulWatch watch = watches.get(listener);
        if (watch != null) {
            watch.stop();
        }
    }

    @Override
    public void close() {
        watches.forEach((listener, watch) -> watch.stop());
        sessions.forEach((listener, session) -> session.close());
    }

    /**
     * Add a listener to a @link{CloudnameService} instance to monitor the creation of services
     * in Cloudname.
     *
     * <p>This is almost a circular dependency but not quite; the service library depends on the
     * core cloudname library (which this library implements and depends on) but it is a marginal
     * layer violation since this library implements the primitives the service library builds
     * upon. It looks weird but luckily we've got lambads and listeners everywhere.
     *
     * <p>Some caution should be taken wrt locking: Since we're skipping across implementation
     * layers and ultimately biting our own tail when using the callbacks we might end up in a
     * deadlock if there's locks in both layers. In short: Avoid locking stuff in the listener's
     * implementation.
     */
    public void monitorLocalServices(final CloudnameService cloudnameService) {
        cloudnameService.registerServiceListener(new LocalServiceListener() {
            @Override
            public void serviceCreated(
                    final ServiceCoordinate coordinate, final Endpoint endpoint) {

                // Note that the service's health check is set to the key that represents
                // the (permanent) service. If someone removes the key, ie unregisters it
                // from Cloudname it will be marked as unhealthy.
                consul.createService(coordinate.toCanonicalString(),
                        endpoint.getName(), endpoint.getHost(), endpoint.getPort(),
                        pathToEphemeralKv(coordinate.toCloudnamePath()));
            }

            @Override
            public void serviceRemoved(final ServiceCoordinate coordinate) {
                consul.removeService(coordinate.toCanonicalString());
            }

            @Override
            public void endpointUpdate(
                    final ServiceCoordinate coordinate, final Endpoint endpoint) {
                consul.updateService(coordinate.toCanonicalString(),
                        endpoint.getName(), endpoint.getHost(), endpoint.getPort());
            }

            @Override
            public void instanceCreated(
                    final InstanceCoordinate coordinate, final Collection<Endpoint> endpoints) {

                // Do the same trickery here and set the service's health check to the ephemeral
                // key that represents the service. If the service terminates then service will
                // be marked unhealthy and reaped within 48 hours by Consul itself.
                final String ephemeralPath
                        = pathToEphemeralKv(coordinate.toCloudnamePath());

                endpoints.forEach((endpoint) ->
                    consul.createService(coordinate.toCanonicalString(),
                            endpoint.getName(), endpoint.getHost(), endpoint.getPort(),
                            ephemeralPath));
            }

            @Override
            public void instanceRemoved(final InstanceCoordinate coordinate) {
                consul.removeService(coordinate.toCanonicalString());
            }

            @Override
            public void instanceEndpointAdded(
                    final InstanceCoordinate coordinate, final Endpoint endpoint) {
                consul.updateService(coordinate.toCanonicalString(),
                        endpoint.getName(), endpoint.getHost(), endpoint.getPort());
            }

            @Override
            public void instanceEndpointRemoved(
                    final InstanceCoordinate coordinate, final Endpoint endpoint) {
                consul.removeService(coordinate.toCanonicalString());
            }
        });
    }
}
