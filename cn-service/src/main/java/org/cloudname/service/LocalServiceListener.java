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
     * Service is created successfully.
     *
     * @param coordinate The service's coordinate
     * @param endpoint The service's data
     */
    void serviceCreated(ServiceCoordinate coordinate, Endpoint endpoint);

    /**
     * Service is removed successfully.
     * @param coordinate
     */
    void serviceRemoved(ServiceCoordinate coordinate);

    /**
     * Service data is updated.
     */
    void endpointUpdate(ServiceCoordinate coordinate, Endpoint endpoint);

    void instanceCreated(InstanceCoordinate coordinate, Collection<Endpoint> endpoints);

    void instanceRemoved(InstanceCoordinate coordinate);

    void instanceEndpointAdded(InstanceCoordinate coordinate, Endpoint endpoint);

    void instanceEndpointRemoved(InstanceCoordinate coordinate, Endpoint endpoint);
}
