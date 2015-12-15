package org.cloudname.service;

import java.util.Collection;

/**
 * Local listener for <strong>all</strong> service events. The events are triggered when clients
 * add, update and remove service entries. If a backend supports some kind of high-level service
 * concept the backend can attach this listener to create corresponding service entries.
 * Since there are two kinds of services - permanent and temporary there's two sets of callbacks.
 */
public interface LocalServiceListener {
    /**
     * Permanent service entry is created.
     */
    void serviceCreated(ServiceCoordinate coordinate, Endpoint endpoint);

    /**
     * Permenet service entry is removed.
     */
    void serviceRemoved(ServiceCoordinate coordinate);

    /**
     * Endpoint for permament service entry is updated.
     */
    void endpointUpdate(ServiceCoordinate coordinate, Endpoint endpoint);

    /**
     * A (regular) service instance is created.
     */
    void instanceCreated(InstanceCoordinate coordinate, Collection<Endpoint> endpoints);

    /**
     * A service instance is removed.
     */
    void instanceRemoved(InstanceCoordinate coordinate);

    /**
     * An endpoint is added to a service.
     */
    void instanceEndpointAdded(InstanceCoordinate coordinate, Endpoint endpoint);

    /**
     * An endpoint is removed from a service.
     */
    void instanceEndpointRemoved(InstanceCoordinate coordinate, Endpoint endpoint);
}
