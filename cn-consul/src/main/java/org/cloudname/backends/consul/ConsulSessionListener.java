package org.cloudname.backends.consul;

/**
 * Session listener. Invoked when session has expired (and the leases are invalidated).
 * the client must be notified of a server failure.
 */
public interface ConsulSessionListener {
    /**
     * Session is expired.
     * @param sessionId The session id
     */
    void sessionExpired(String sessionId);
}
