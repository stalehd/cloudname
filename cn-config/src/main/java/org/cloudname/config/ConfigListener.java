package org.cloudname.config;

public interface ConfigListener {
    boolean configUpdated(final Config newConfig);
    void configRemoved();
}
