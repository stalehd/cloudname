package org.cloudname.service;

/**
 *
 */
public interface LocalServiceHandleListener {
    void endpointAdded(Endpoint endpoint);
    void endpointRemoved(Endpoint endpoint);
    void handleClosed();
}
