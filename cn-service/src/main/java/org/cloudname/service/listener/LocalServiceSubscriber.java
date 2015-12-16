package org.cloudname.service.listener;

/**
 * Subscriber interface for local service events. If the backend implements this interface
 * the @link{CloudnameService} instance will attach the listener to itself when it is created.
 */
public interface LocalServiceSubscriber {
    /**
     * Get the listener for local services.
     */
    LocalServiceListener getLocalServiceListener();
}
