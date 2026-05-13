package com.fiserv.fef.jobs;

import java.io.Serializable;
import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration for Main Validation Job
 */
public class JobConfig implements Serializable {

    private static final long serialVersionUID = 1L;
    private String mongoUri;
    private String mongoDatabase;        // SEP data database
    private String mongoFefDatabase;     // FEF collections database
    private int pollingIntervalSeconds;
    private long checkpointIntervalMs;
    private String flinkJobManager;
    private String workerJobJar;
    private int workerJobTimeoutMinutes; // Timeout for worker job in minutes
    private int flinkDefaultParallelism; // Default parallelism for Flink environment
    private int maxTriggersPerPoll; // Maximum triggers to pick per poll
    private int maxConcurrentPipelines; // Maximum pipelines to process concurrently
    private int workerThreadPoolSize; // Thread pool size for worker job submissions
    // MongoDB Collection Names
    private String pipelineCollection;
    private String registryCollection;
    private String resultsCollection;
    private String featuresCollection;
    private String scoreCollection;

    /**
     * Represents a topic and its groupId.
     */
    public static class TopicGroup implements Serializable {
        private static final long serialVersionUID = 1L;

        public final String topic;
        public final String groupId;

        public TopicGroup(String topic, String groupId) {
            this.topic = topic;
            this.groupId = groupId;
        }
    }

    public JobConfig() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                props.load(input);
            }
        } catch (Exception e) {
            System.err.println("Failed to load application.properties: " + e.getMessage());
        }

        // Helper method to resolve property values with environment variable fallback
        this.mongoUri = resolveProperty(props, "mongodb.uri", "MONGODB_URI", "mongodb://localhost:27017");
        this.mongoDatabase = resolveProperty(props, "mongodb.database", "MONGODB_DATABASE", "Optis_Dev");
        this.mongoFefDatabase = resolveProperty(props, "mongodb.fef.database", "MONGODB_FEF_DATABASE", "Optis_fef_config_db");
        this.pollingIntervalSeconds = Integer.parseInt(resolveProperty(props, "main.job.polling.interval", null, "10"));
        this.checkpointIntervalMs = Long.parseLong(resolveProperty(props, "flink.checkpoint.interval", null, "60000"));
        this.flinkJobManager = resolveProperty(props, "flink.jobmanager.address", "FLINK_JOBMANAGER", "localhost:8081");
        this.flinkJobManager = inferFlinkJobManagerIfDefault(this.flinkJobManager);
        this.workerJobJar = resolveProperty(props, "worker.job.jar.path", null, "/work/fef-worker-job.jar");
        this.workerJobTimeoutMinutes = Integer.parseInt(resolveProperty(props, "worker.job.timeout", null, "600000")) / 60000;
        this.flinkDefaultParallelism = Integer.parseInt(resolveProperty(props, "flink.default.parallelism", null, "4"));
        this.maxTriggersPerPoll = Integer.parseInt(resolveProperty(props, "pipeline.max.triggers.per.poll", null, "10"));
        this.maxConcurrentPipelines = Integer.parseInt(resolveProperty(props, "pipeline.max.concurrent.pipelines", null, "5"));
        this.workerThreadPoolSize = Integer.parseInt(resolveProperty(props, "worker.job.thread.pool.size", null, "20"));

        // MongoDB Collection Names
        this.pipelineCollection = resolveProperty(props, "mongodb.collection.pipeline", "MONGODB_COLLECTION_PIPELINE", "fef_validation_pipeline");
        this.registryCollection = resolveProperty(props, "mongodb.collection.registry", "MONGODB_COLLECTION_REGISTRY", "fef_validation_registry");
        this.resultsCollection = resolveProperty(props, "mongodb.collection.results", "MONGODB_COLLECTION_RESULTS", "fef_validation_results");
        this.featuresCollection = resolveProperty(props, "mongodb.collection.features", "MONGODB_COLLECTION_FEATURES", "fef_validation_features");
        this.scoreCollection = resolveProperty(props, "mongodb.collection.scores", "MONGODB_COLLECTION_SCORES", "fef_validation_scores");
    }

    /**
     * OpenShift/Kubernetes helper:
     * If flinkJobManager is left as the default localhost:8081, try to infer a usable in-cluster service DNS.
     *
     * This is best-effort and only kicks in when users did not (or cannot) set FLINK_JOBMANAGER / --flink-jobmanager.
     *
     * Strategy:
     * 1) If a standard Flink service env var exists (e.g., FLINK_JOBMANAGER_SERVICE_HOST/PORT), use it.
     * 2) Otherwise, scan env for *JOBMANAGER*_SERVICE_HOST/PORT and use the first match.
     */
    private String inferFlinkJobManagerIfDefault(String current) {
        if (current == null) {
            return null;
        }

        String trimmed = current.trim();
        if (!"localhost:8081".equalsIgnoreCase(trimmed) && !"127.0.0.1:8081".equalsIgnoreCase(trimmed)) {
            return current;
        }

        // 1) Explicit common env vars
        String host = System.getenv("FLINK_JOBMANAGER_SERVICE_HOST");
        String port = System.getenv("FLINK_JOBMANAGER_SERVICE_PORT");
        if (isNonBlank(host)) {
            if (!isNonBlank(port)) {
                port = "8081";
            }
            return host.trim() + ":" + port.trim();
        }

        // 2) Best-effort scan for any *JOBMANAGER*_SERVICE_HOST paired with *_SERVICE_PORT
        for (String key : System.getenv().keySet()) {
            if (key == null) {
                continue;
            }
            String upper = key.toUpperCase();
            if (upper.endsWith("_SERVICE_HOST") && upper.contains("JOBMANAGER")) {
                String candidateHost = System.getenv(key);
                if (!isNonBlank(candidateHost)) {
                    continue;
                }

                String portKey = key.substring(0, key.length() - "_HOST".length()) + "PORT";
                String candidatePort = System.getenv(portKey);
                if (!isNonBlank(candidatePort)) {
                    candidatePort = "8081";
                }

                return candidateHost.trim() + ":" + candidatePort.trim();
            }
        }

        return current;
    }

    private boolean isNonBlank(String s) {
        return s != null && !s.trim().isEmpty();
    }

    /**
     * Resolve property value with support for ${ENV_VAR:defaultValue} pattern
     * @param props Properties object
     * @param propKey Property key
     * @param envKey Environment variable key (optional)
     * @param defaultValue Default value if not found
     * @return Resolved value
     */
    private String resolveProperty(Properties props, String propKey, String envKey, String defaultValue) {
        String value = props.getProperty(propKey);

        // If property value contains ${...} pattern, resolve it
        if (value != null && value.contains("${")) {
            // Extract pattern: ${ENV_VAR:defaultValue}
            int start = value.indexOf("${");
            int end = value.indexOf("}", start);
            if (start >= 0 && end > start) {
                String pattern = value.substring(start + 2, end);
                String[] parts = pattern.split(":", 2);
                String envVar = parts[0];
                String fallback = parts.length > 1 ? parts[1] : defaultValue;

                // Try to get from environment
                String envValue = System.getenv(envVar);
                value = (envValue != null) ? envValue : fallback;
            }
        }

        // If still null/empty, try environment variable directly
        if ((value == null || value.trim().isEmpty()) && envKey != null) {
            value = System.getenv(envKey);
        }

        // Finally, use default value
        return (value != null && !value.trim().isEmpty()) ? value : defaultValue;
    }

    // Getters and Setters
    public String getMongoUri() { return mongoUri; }
    public void setMongoUri(String mongoUri) { this.mongoUri = mongoUri; }

    public String getMongoDatabase() { return mongoDatabase; }
    public void setMongoDatabase(String mongoDatabase) { this.mongoDatabase = mongoDatabase; }

    public int getPollingIntervalSeconds() { return pollingIntervalSeconds; }
    public void setPollingIntervalSeconds(int pollingIntervalSeconds) {
        this.pollingIntervalSeconds = pollingIntervalSeconds;
    }

    public String getMongoFefDatabase() { return mongoFefDatabase; }
    public void setMongoFefDatabase(String mongoFefDatabase) {
        this.mongoFefDatabase = mongoFefDatabase;
    }

    public long getCheckpointIntervalMs() { return checkpointIntervalMs; }
    public void setCheckpointIntervalMs(long checkpointIntervalMs) {
        this.checkpointIntervalMs = checkpointIntervalMs;
    }

    public String getFlinkJobManager() { return flinkJobManager; }
    public void setFlinkJobManager(String flinkJobManager) {
        this.flinkJobManager = flinkJobManager;
    }

    public String getWorkerJobJar() { return workerJobJar; }
    public void setWorkerJobJar(String workerJobJar) {
        this.workerJobJar = workerJobJar;
    }

    public int getWorkerJobTimeoutMinutes() { return workerJobTimeoutMinutes; }
    public void setWorkerJobTimeoutMinutes(int workerJobTimeoutMinutes) {
        this.workerJobTimeoutMinutes = workerJobTimeoutMinutes;
    }

    public int getFlinkDefaultParallelism() { return flinkDefaultParallelism; }
    public void setFlinkDefaultParallelism(int flinkDefaultParallelism) {
        this.flinkDefaultParallelism = flinkDefaultParallelism;
    }

    public int getMaxTriggersPerPoll() { return maxTriggersPerPoll; }
    public void setMaxTriggersPerPoll(int maxTriggersPerPoll) {
        this.maxTriggersPerPoll = maxTriggersPerPoll;
    }

    public int getMaxConcurrentPipelines() { return maxConcurrentPipelines; }
    public void setMaxConcurrentPipelines(int maxConcurrentPipelines) {
        this.maxConcurrentPipelines = maxConcurrentPipelines;
    }

    public int getWorkerThreadPoolSize() { return workerThreadPoolSize; }
    public void setWorkerThreadPoolSize(int workerThreadPoolSize) {
        this.workerThreadPoolSize = workerThreadPoolSize;
    }

    public String getPipelineCollection() { return pipelineCollection; }
    public void setPipelineCollection(String pipelineCollection) {
        this.pipelineCollection = pipelineCollection;
    }

    public String getRegistryCollection() { return registryCollection; }
    public void setRegistryCollection(String registryCollection) {
        this.registryCollection = registryCollection;
    }

    public String getResultsCollection() { return resultsCollection; }
    public void setResultsCollection(String resultsCollection) {
        this.resultsCollection = resultsCollection;
    }

    public String getFeaturesCollection() { return featuresCollection; }
    public void setFeaturesCollection(String featuresCollection) {
        this.featuresCollection = featuresCollection;
    }

    public String getScoreCollection() {
        return scoreCollection;
    }

    public void setScoreCollection(String scoreCollection) {
        this.scoreCollection = scoreCollection;
    }

    @Override
    public String toString() {
        return "JobConfig{" +
                "mongoDatabase='" + mongoDatabase + '\'' +
                ", mongoFefDatabase='" + mongoFefDatabase + '\'' +
                ", pollingInterval=" + pollingIntervalSeconds + "s" +
                ", checkpointInterval=" + checkpointIntervalMs + "ms" +
                ", flinkJobManager='" + flinkJobManager + '\'' +
                ", flinkDefaultParallelism=" + flinkDefaultParallelism +
                ", maxTriggersPerPoll=" + maxTriggersPerPoll +
                ", maxConcurrentPipelines=" + maxConcurrentPipelines +
                ", workerThreadPoolSize=" + workerThreadPoolSize +
                ", workerJobTimeoutMinutes=" + workerJobTimeoutMinutes +
                '}';
    }
}
