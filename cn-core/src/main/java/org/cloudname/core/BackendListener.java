package org.cloudname.core;

/**
 * Listener class for the backend itself.
 */
public interface BackendListener {
    /**
     * Callback when backend becomes available. The backend is operating normally, at least the
     * local interface of the backend.
     */
    void backendIsAvailable();

    /**
     * Callback when the backend has become unavailable. Normally this means that leases might
     * have expired for this client and new leases should be acquired. The current state of the
     * existing leases might be unknown at this point.
     */
    void backendIsUnavailable();

}
