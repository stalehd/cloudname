package org.cloudname.core;

/**
 * Listener for backend availability events.
 */
public interface AvailabilityListener {
    /**
     * The backend are either AVAILABLE with a quorum or UNAVAILABLE without a quorum (or network
     * connectivity). Some backends might announce a READ_ONLY state where leases can be read but
     * they can't be updated.
     */
    enum State {
        AVAILABLE, UNAVAILABLE, READ_ONLY
    }

    void availabilityChange(State newState);
}
