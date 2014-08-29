package org.glue.modules.hadoop;

import org.apache.hadoop.conf.Configuration

/**
 * 
 * Interface for when a class is per-cluster configured.
 * 
 */
public interface ClusterConfigured {
    
    /**
     * Get the hadoop configuration for the specified cluster, or returns null.<br/>
     * null clusterName must be checked explicitly; it is up to the implementation to load default config or return null.
     */
    Configuration getClusterConf(String clusterName);
    
}
