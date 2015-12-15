package org.cloudname.service;

/**
 * A listener for the local service handle. It gets callbacks whenever the client updates the
 * service handle with endpoints and when the handle is closed.
 *
 * @author stalehd@gmail.com
 */
public interface LocalServiceHandleListener {

    /**
     * Endpoint is added.
     */
    void endpointAdded(Endpoint endpoint);

    /**
     * Endpoint is removed.
     */
    void endpointRemoved(Endpoint endpoint);

    /**
     * The @link{ServiceHandle} is closed.
     */
    void handleClosed();
}
