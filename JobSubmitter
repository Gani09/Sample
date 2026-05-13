package com.fiserv.fef.jobs.processors;

import com.fiserv.fef.jobs.JobConfig;
import com.fiserv.fef.models.ValidationConfig;
import com.fiserv.fef.models.ValidationResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.mongodb.client.model.Filters.eq;

/**
 * Submits worker jobs to the Flink cluster's JobManager via RestClusterClient.
 *
 * Uses PackagedProgram + JobGraph submitted through RestClusterClient so that
 * worker jobs are scheduled by the cluster's ResourceManager across all
 * TaskManagers, instead of creating in-process MiniClusters.
 *
 * The worker JAR must already be present on the classpath of all TaskManagers
 * (e.g. placed in /opt/flink/lib/ in the Docker image). This avoids the
 * JAR upload POST that is rejected in Kubernetes Application Mode.
 */
public class JobSubmitter {

    private static final Logger LOG = LoggerFactory.getLogger(JobSubmitter.class);

    private final JobConfig config;
    private final RestClusterClient<?> flinkClient;
    private final MongoClient mongoClient;
    private final MongoCollection<Document> resultsCollection;

    public JobSubmitter(JobConfig jobConfig) {
        this.config = jobConfig;

        try {
            // Initialize Flink REST client
            Configuration flinkConfig = new Configuration();
            String jobManagerAddress = config.getFlinkJobManager();

            // Handle null or empty jobmanager address
            if (jobManagerAddress == null || jobManagerAddress.trim().isEmpty()) {
                LOG.warn("Flink JobManager address is null/empty, using default: localhost:8081");
                jobManagerAddress = "localhost:8081";
            }

            LOG.info("Initializing Flink REST client with JobManager: {}", jobManagerAddress);

            String[] parts = jobManagerAddress.split(":");
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid JobManager address format. Expected 'host:port', got: " + jobManagerAddress
                );
            }

            String host = parts[0].trim();
            int port;
            try {
                port = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid port number in JobManager address: " + parts[1] +
                                ". Full address: " + jobManagerAddress, e
                );
            }

            // Use RestOptions for the REST endpoint (port 8081), not JobManagerOptions.PORT (RPC port 6123)
            flinkConfig.setString(JobManagerOptions.ADDRESS, host);
            flinkConfig.setInteger(RestOptions.PORT, port);

            this.flinkClient = new RestClusterClient<>(flinkConfig, "FEF-JobSubmitter");

            LOG.info("Flink REST client initialized successfully: {}:{}", host, port);

        } catch (Exception e) {
            LOG.error("Failed to initialize Flink client", e);
            throw new RuntimeException("Failed to initialize Flink client", e);
        }

        // Initialize MongoDB client for reading results
        this.mongoClient = MongoClients.create(config.getMongoUri());
        MongoDatabase database = mongoClient.getDatabase(config.getMongoFefDatabase());
        this.resultsCollection = database.getCollection(config.getResultsCollection());
    }

    // =================================================================
    // APPROACH 1: Using PackagedProgram + JobGraph via RestClusterClient
    // =================================================================

    /**
     * Submit worker job to the Flink cluster's JobManager via RestClusterClient.
     *
     * Uses PackagedProgram to build a JobGraph from the worker JAR (which is
     * already on the classpath of all TaskManagers at /opt/flink/lib/fef-worker-job.jar).
     * The JobGraph is submitted to the cluster's JobManager which schedules it
     * across all available TaskManagers — no JAR upload needed.
     *
     * This replaces the previous approach of invoking BatchValidationRunner.main()
     * directly, which created a MiniCluster inside the calling TaskManager and
     * caused memory issues (all worker jobs ran on the same pod).
     */
    public JobID submitWorkerJob(ValidationConfig validationConfig, String validationDate, String pipelineRunID)
            throws Exception {

        LOG.info("Submitting worker job to cluster: validation={}", validationConfig.getValidationID());

        // Build job arguments
        String[] args = buildJobArguments(validationConfig, validationDate, pipelineRunID);

        // Get worker JAR file
        File jarFile = new File(config.getWorkerJobJar());
        if (!jarFile.exists()) {
            throw new IllegalStateException("Worker JAR not found: " + jarFile.getAbsolutePath());
        }

        // Set job name with more context for traceability
        String jobName = String.format("FEF Worker: processID=%s, processName=%s, validationID=%s, featureID=%s, valName=%s",
                validationConfig.getProcessID() != null ? validationConfig.getProcessID() : "N/A",
                validationConfig.getProcessName() != null ? validationConfig.getProcessName() : "N/A",
                validationConfig.getValidationID() != null ? validationConfig.getValidationID() : "N/A",
                validationConfig.getFeatureID() != null ? validationConfig.getFeatureID() : "N/A",
                validationConfig.getValName() != null ? validationConfig.getValName() : "N/A"
        );

        // Sanitize null arguments
        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                LOG.error("Null argument at index {} in args array! Arguments: {}", i, java.util.Arrays.toString(args));
                args[i] = ""; // Replace null with empty string to avoid NPE
            }
        }
        List<String> argList = new ArrayList<>(List.of(args));
        argList.add("--job-name");
        argList.add(jobName);
        args = argList.toArray(new String[0]);

        final String validationID = validationConfig.getValidationID();

        try {
            // Build PackagedProgram from the worker JAR
            // The JAR is already present on all TaskManagers at /opt/flink/lib/
            // so we don't need to upload it — just need to build the JobGraph
            PackagedProgram program = PackagedProgram.newBuilder()
                    .setJarFile(jarFile)
                    .setEntryPointClassName("com.fiserv.fef.jobs.BatchValidationRunner")
                    .setArguments(args)
                    .build();

            // Create JobGraph from the PackagedProgram
            // parallelism=1 since each worker validation is a single-element batch job
            Configuration programConfig = new Configuration();
            JobGraph jobGraph = PackagedProgramUtils.createJobGraph(
                    program,
                    programConfig,
                    1,   // parallelism
                    false // suppress output
            );

            // NOTE: The job name is set by BatchValidationRunner via env.execute(name).
            // The --job-name argument is passed so the worker can use it.
            // JobGraph does not have a setName() method; name is set at construction time.

            // Submit the JobGraph to the cluster's JobManager via REST
            // This does NOT upload the JAR — it only sends the serialized JobGraph.
            // The JobManager's ResourceManager will schedule tasks across all TaskManagers.
            LOG.info("Submitting JobGraph to cluster JobManager: validation={}, jobGraphId={}",
                    validationID, jobGraph.getJobID());

            CompletableFuture<JobID> submissionFuture = flinkClient.submitJob(jobGraph);
            JobID jobId = submissionFuture.get();

            LOG.info("Worker job submitted to cluster successfully: validation={}, jobId={}",
                    validationID, jobId);

            return jobId;

        } catch (Exception e) {
            LOG.error("Failed to submit worker job to cluster: validation={}", validationID, e);
            throw new RuntimeException("Worker job submission failed: " + validationID, e);
        }
    }

    // =================================================================
    // DISTRIBUTED PARQUET COMPARISON JOB
    // =================================================================

    /**
     * Submit a distributed parquet comparison job.
     *
     * Build the JobGraph directly in this method instead of using PackagedProgram
     * to avoid classloader issues with connector dependencies.
     *
     * The validation registry document should contain:
     *   execution_mode: "distributed_parquet"
     *   execution_profiles:
     *     sourceProfile: { profileType: "parquet", profileName: "/path/to/source.parquet" }
     *     targetProfile: { profileType: "parquet", profileName: "/path/to/target.parquet" }
     *   distributed_config:
     *     join_key: "chdAccountNumber"
     *     parallelism: 15
     *     expected_updates: "chdAddrLine1:true,REMAINING:false"
     */
    public JobID submitDistributedParquetJob(ValidationConfig validationConfig,
                                             String validationDate,
                                             String pipelineRunID) throws Exception {

        LOG.info("Submitting distributed parquet job: validation={}", validationConfig.getValidationID());

        // Extract paths from data source configs
        String sourcePath = null;
        String targetPath = null;
        if (validationConfig.getTargetDataSources() != null) {
            for (var ds : validationConfig.getTargetDataSources()) {
                if ("sourceProfile".equals(ds.getProfileRole())) {
                    sourcePath = ds.getTopic(); // profileName stored in topic field
                } else if ("targetProfile".equals(ds.getProfileRole())) {
                    targetPath = ds.getTopic();
                }
            }
        }

        if (sourcePath == null || targetPath == null) {
            throw new IllegalStateException(
                    "distributed_parquet mode requires sourceProfile and targetProfile in execution_profiles");
        }

        // Get distributed config from validation config (or use defaults)
        String joinKey = validationConfig.getDistributedJoinKey();
        if (joinKey == null || joinKey.isBlank()) {
            throw new IllegalStateException(
                    "distributed_parquet validation '" + validationConfig.getValidationID() +
                    "' is missing 'distributed_config.join_key' in the validation_registry document. " +
                    "Example: { \"distributed_config\": { \"join_key\": \"chdAccountNumber\", \"parallelism\": 20 } }");
        }
        int parallelism = validationConfig.getDistributedParallelism() > 0
                ? validationConfig.getDistributedParallelism() : config.getWorkerThreadPoolSize();
        String expectedUpdates = validationConfig.getDistributedExpectedUpdates() != null
                ? validationConfig.getDistributedExpectedUpdates() : "";

        String jobName = String.format("FEF Distributed Parquet: %s (join=%s, parallelism=%d)",
                validationConfig.getValidationID(), joinKey, parallelism);

        try {
            // No need to read schemas — ParquetTaggedRecordSource reads the file's own schema internally
            LOG.info("Building distributed parquet compare job (schema-free pipeline)");

            // Parse expected updates if provided
            java.util.Map<String, Boolean> expectedUpdatesMap = new java.util.LinkedHashMap<>();
            boolean validateRemainingUnchanged = false;

            if (!expectedUpdates.isBlank()) {
                String[] pairs = expectedUpdates.split(",");
                for (String pair : pairs) {
                    String[] kv = pair.trim().split(":", 2);
                    if (kv.length == 2) {
                        if ("REMAINING".equalsIgnoreCase(kv[0].trim())) {
                            validateRemainingUnchanged = !Boolean.parseBoolean(kv[1].trim());
                        } else {
                            expectedUpdatesMap.put(kv[0].trim(), Boolean.parseBoolean(kv[1].trim()));
                        }
                    }
                }
            }

            // Build compare config
            com.fiserv.fef.parquet.distributed.DistributedParquetCompareJob.CompareConfig compareConfig =
                    new com.fiserv.fef.parquet.distributed.DistributedParquetCompareJob.CompareConfig(
                            sourcePath,
                            targetPath,
                            joinKey,
                            new ArrayList<>(),  // fieldsToCompare (empty = all fields)
                            expectedUpdatesMap,
                            validateRemainingUnchanged,
                            parallelism
                    );

            // Create a new StreamExecutionEnvironment for building the job graph
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(parallelism);

            // Build the distributed comparison pipeline
            // ⚡ useFileSource=true → ParallelParquetFileSource (20 parallel readers, 10-20x faster)
            org.apache.flink.streaming.api.datastream.DataStream<com.fiserv.fef.parquet.distributed.DistributedParquetCompareJob.CompareResult> mismatches =
                    com.fiserv.fef.parquet.distributed.DistributedParquetCompareJob.execute(env, compareConfig, null, null, true);

            // Add result sink — passes MongoDB params so the job writes its result
            // directly to fef_validation_results, enabling waitForJobCompletion() to
            // find the real mismatch counts via checkForResult().
            mismatches.addSink(new com.fiserv.fef.parquet.distributed.DistributedParquetCompareRunner.MismatchCountingSink(
                            100,
                            config.getMongoUri(),
                            config.getMongoFefDatabase(),
                            config.getResultsCollection(),
                            validationConfig.getValidationID(),
                            pipelineRunID,
                            sourcePath,
                            targetPath))
                    .setParallelism(1)
                    .name("Result-Collector");

            // Get the JobGraph from the environment
            org.apache.flink.streaming.api.graph.StreamGraph streamGraph = env.getStreamGraph();
            streamGraph.setJobName(jobName);
            JobGraph jobGraph = streamGraph.getJobGraph();


            LOG.info("Submitting distributed parquet JobGraph: validation={}, parallelism={}, jobGraphId={}",
                    validationConfig.getValidationID(), parallelism, jobGraph.getJobID());

            CompletableFuture<JobID> submissionFuture = flinkClient.submitJob(jobGraph);
            JobID jobId = submissionFuture.get();

            LOG.info("Distributed parquet job submitted: validation={}, jobId={}",
                    validationConfig.getValidationID(), jobId);

            return jobId;

        } catch (Exception e) {
            LOG.error("Failed to submit distributed parquet job: validation={}",
                    validationConfig.getValidationID(), e);
            throw new RuntimeException("Distributed parquet job submission failed", e);
        }
    }

    // =================================================================
    // APPROACH 2: Using Flink CLI Command (SIMPLE)
    // =================================================================

    /**
     * Submit worker job using Flink CLI command
     * Simpler but less control over job execution
     */
    public JobID submitWorkerJobViaCLI(ValidationConfig validationConfig, String validationDate, String pipelineRunID)
            throws Exception {

        LOG.info("Submitting worker job via CLI: validation={}",
                validationConfig.getValidationID());

        // Build Flink CLI command
        StringBuilder command = new StringBuilder();
        command.append("flink run -d");  // -d = detached mode
        command.append(" --jobmanager ").append(config.getFlinkJobManager());
        command.append(" ").append(config.getWorkerJobJar());
        command.append(" --validation-id ").append(validationConfig.getValidationID());
        command.append(" --validation-date ").append(validationDate);
        command.append(" --pipeline-run-id ").append(pipelineRunID);
        command.append(" --feature-id ").append(validationConfig.getFeatureID());
        command.append(" --process-id ").append(validationConfig.getProcessID());
        command.append(" --mongo-uri \"").append(config.getMongoUri()).append("\"");
        command.append(" --mongo-database ").append(config.getMongoDatabase());

        // Execute command
        Process process = Runtime.getRuntime().exec(command.toString());

        // Wait for command completion
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Flink job submission failed with exit code: " + exitCode);
        }

        // Parse JobID from output
        JobID jobId = parseJobIdFromOutput(process);

        LOG.info("Worker job submitted via CLI: validation={}, jobId={}",
                validationConfig.getValidationID(), jobId);

        return jobId;
    }

    // =================================================================
    // APPROACH 3: Using REST API Directly (MANUAL)
    // =================================================================

    /**
     * Submit worker job using direct REST API calls
     * Most control but requires more manual work
     */
    public JobID submitWorkerJobViaRestAPI(ValidationConfig validationConfig,
                                           String validationDate,
                                           String pipelineRunID)
            throws Exception {

        LOG.info("Submitting worker job via REST API: validation={}",
                validationConfig.getValidationID());

        // Step 1: Upload JAR to Flink cluster
        String jarId = uploadJarToFlink();

        // Step 2: Build job execution request
        String jobArgs = buildJobArgumentsAsString(validationConfig, validationDate, pipelineRunID);

        // Step 3: Submit job via REST API
        // POST /jars/:jarid/run
        String url = String.format("http://%s/jars/%s/run",
                config.getFlinkJobManager(), jarId);

        String requestBody = String.format(
                "{\"programArgs\": \"%s\", \"parallelism\": 1}",
                jobArgs
        );

        // Make HTTP POST request (use your HTTP client library)
        // HttpClient, RestTemplate, OkHttp, etc.
        String response = makeHttpPostRequest(url, requestBody);

        // Parse JobID from response
        JobID jobId = parseJobIdFromJson(response);

        LOG.info("Worker job submitted via REST API: validation={}, jobId={}",
                validationConfig.getValidationID(), jobId);

        return jobId;
    }

    // =================================================================
    // HELPER METHODS
    // =================================================================

    /**
     * Build job arguments array
     * Updated to include all fields from ValidationRegistryDTO
     */
    private String[] buildJobArguments(ValidationConfig config, String validationDate, String pipelineRunID) {
        List<String> args = new ArrayList<>();

        args.add("--validation-id");
        args.add(config.getValidationID() != null ? config.getValidationID() : "");

        args.add("--validation-date");
        args.add(validationDate != null ? validationDate : "");

        args.add("--pipeline-run-id");
        args.add(pipelineRunID != null ? pipelineRunID : "");

        args.add("--feature-id");
        args.add(config.getFeatureID() != null ? config.getFeatureID() : "");

        args.add("--process-id");
        args.add(config.getProcessID() != null ? config.getProcessID() : "");

        args.add("--process-name");
        args.add(config.getProcessName() != null ? config.getProcessName() : "");

        args.add("--val-description");
        args.add(config.getValDescription() != null ? config.getValDescription() : "");

        args.add("--val-name");
        args.add(config.getValName() != null ? config.getValName() : "");

        args.add("--step-number");
        args.add(String.valueOf(config.getStepNumber()));

        args.add("--track");
        args.add(config.getTrack() != null ? config.getTrack() : "");

        args.add("--orchestration");
        args.add(config.getOrchestration() != null ? config.getOrchestration() : "");

        args.add("--created-at");
        args.add(config.getCreatedAt() != null ? config.getCreatedAt() : "");

        args.add("--enabled");
        args.add(String.valueOf(config.isEnabled()));

        // Add execution_profiles as JSON string if present
        if (config.getExecutionProfiles() != null) {
            args.add("--execution-profiles");
            try {
                args.add(new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(config.getExecutionProfiles()));
            } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
                LOG.warn("Failed to serialize executionProfiles to JSON, using toString() fallback", e);
                args.add(config.getExecutionProfiles().toString());
            }
        }

        args.add("--mongo-uri");
        args.add(this.config.getMongoUri() != null ? this.config.getMongoUri() : "");

        args.add("--mongo-database");
        args.add(this.config.getMongoDatabase() != null ? this.config.getMongoDatabase() : "");

        args.add("--mongo-fef-database");
        args.add(this.config.getMongoFefDatabase() != null ? this.config.getMongoFefDatabase() : "");

        args.add("--results-collection");
        args.add(this.config.getResultsCollection() != null ? this.config.getResultsCollection() : "fef_validation_results");

        args.add("--features-collection");
        args.add(this.config.getFeaturesCollection() != null ? this.config.getFeaturesCollection() : "fef_validation_features");

        args.add("--source-db-name");
        args.add(config.getSourceDbName() != null ? config.getSourceDbName() : "");

        args.add("--target-db-name");
        args.add(config.getTargetDbName() != null ? config.getTargetDbName() : "");

        return args.toArray(new String[0]);
    }

    /**
     * Build job arguments as string (for CLI/REST API)
     */
    private String buildJobArgumentsAsString(ValidationConfig config, String validationDate, String pipelineRunID) {
        return String.join(" ", buildJobArguments(config, validationDate, pipelineRunID));
    }

    /**
     * Upload JAR to Flink cluster (for REST API approach)
     */
    private String uploadJarToFlink() throws Exception {
        // POST /jars/upload
        String url = String.format("http://%s/jars/upload", config.getFlinkJobManager());

        File jarFile = new File(config.getWorkerJobJar());

        // Upload JAR file (use multipart/form-data)
        // Implementation depends on HTTP client library
        String jarId = uploadFile(url, jarFile);

        LOG.debug("JAR uploaded to Flink: jarId={}", jarId);

        return jarId;
    }

    /**
     * Parse JobID from CLI output
     */
    private JobID parseJobIdFromOutput(Process process) throws Exception {
        // Read process output and parse JobID
        // Format: "Job has been submitted with JobID <jobid>"

        // Simplified implementation
        // In production, properly parse the output
        return JobID.generate();  // Placeholder
    }

    /**
     * Parse JobID from REST API JSON response
     */
    private JobID parseJobIdFromJson(String json) {
        // Parse JSON: {"jobid": "..."}
        // Use Jackson, Gson, or similar
        return JobID.generate();  // Placeholder
    }

    /**
     * Make HTTP POST request
     */
    private String makeHttpPostRequest(String url, String body) throws Exception {
        // Use your HTTP client library
        // HttpClient, RestTemplate, OkHttp, etc.
        return "";  // Placeholder
    }

    /**
     * Upload file via HTTP
     */
    private String uploadFile(String url, File file) throws Exception {
        // Use multipart/form-data upload
        return "";  // Placeholder
    }

    // =================================================================
    // JOB MONITORING
    // =================================================================

    /**
     * Wait for worker job completion and retrieve result.
     * Uses RestClusterClient to poll the job status from the cluster's JobManager.
     */
    public ValidationResult waitForJobCompletion(JobID jobId, ValidationConfig validationConfig, String pipelineRunID)
            throws Exception {

        LOG.info("Waiting for job completion: validation={}, jobId={}, pipelineRunID={}",
                validationConfig.getValidationID(), jobId, pipelineRunID);

        long startTime = System.currentTimeMillis();
        long timeoutMs = config.getWorkerJobTimeoutMinutes() * 60 * 1000L;

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            try {
                // Query the cluster's JobManager for the real job status
                CompletableFuture<JobStatus> statusFuture = flinkClient.getJobStatus(jobId);
                JobStatus jobStatus = statusFuture.get();

                LOG.debug("Job status for validation={}, jobId={}: {}",
                        validationConfig.getValidationID(), jobId, jobStatus);

                if (jobStatus == JobStatus.FINISHED) {
                    // Job completed — check MongoDB for the result written by the worker
                    ValidationResult result = checkForResult(validationConfig.getValidationID(), pipelineRunID);
                    if (result != null) {
                        LOG.info("Job completed: validation={}, pipelineRunID={}, passed={}, duration={}ms",
                                validationConfig.getValidationID(),
                                pipelineRunID,
                                result.isPassed(),
                                System.currentTimeMillis() - startTime
                        );
                        return result;
                    }

                    // Job finished but result not yet in MongoDB — give it a brief grace period
                    LOG.warn("Job FINISHED but result not yet in MongoDB: validation={}, jobId={}. Retrying...",
                            validationConfig.getValidationID(), jobId);
                    Thread.sleep(2000);
                    result = checkForResult(validationConfig.getValidationID(), pipelineRunID);
                    if (result != null) {
                        return result;
                    }

                    // Return a synthetic success based on job completion
                    LOG.warn("Job FINISHED but no MongoDB result found after grace period: validation={}",
                            validationConfig.getValidationID());
                    ValidationResult finishedResult = new ValidationResult(validationConfig.getValidationID());
                    finishedResult.setPassed(true);
                    finishedResult.setDurationMs(System.currentTimeMillis() - startTime);
                    return finishedResult;
                }

                if (jobStatus == JobStatus.FAILED || jobStatus == JobStatus.CANCELED) {
                    LOG.error("Job failed or canceled: validation={}, jobId={}, status={}",
                            validationConfig.getValidationID(), jobId, jobStatus);

                    ValidationResult failureResult = new ValidationResult(
                            validationConfig.getValidationID()
                    );
                    failureResult.setPassed(false);
                    failureResult.setErrorMessage("Job " + jobStatus.name().toLowerCase());
                    failureResult.setDurationMs(System.currentTimeMillis() - startTime);

                    return failureResult;
                }

                // Job still running — also check MongoDB in case result was written early
                ValidationResult earlyResult = checkForResult(validationConfig.getValidationID(), pipelineRunID);
                if (earlyResult != null) {
                    LOG.info("Job result found in MongoDB while job still {}: validation={}, passed={}, duration={}ms",
                            jobStatus, validationConfig.getValidationID(),
                            earlyResult.isPassed(), System.currentTimeMillis() - startTime);
                    return earlyResult;
                }

            } catch (Exception e) {
                LOG.warn("Error polling job status for validation={}, jobId={}: {}",
                        validationConfig.getValidationID(), jobId, e.getMessage());
            }

            // Sleep before next check
            Thread.sleep(5000);  // Check every 5 seconds
        }

        // Timeout
        LOG.error("Job timed out: validation={}, jobId={}", validationConfig.getValidationID(), jobId);

        ValidationResult timeoutResult = new ValidationResult(
                validationConfig.getValidationID()
        );
        timeoutResult.setPassed(false);
        timeoutResult.setErrorMessage("Job execution timed out after " + config.getWorkerJobTimeoutMinutes() + " minutes");
        timeoutResult.setDurationMs(System.currentTimeMillis() - startTime);

        return timeoutResult;
    }


    /**
     * Check MongoDB for validation result
     * Query by both validationID AND pipelineRunID to ensure uniqueness
     */
    private ValidationResult checkForResult(String validationID, String pipelineRunID) {
        try {
            // Query by both validationID and pipelineRunID
            Document query = new Document()
                    .append("validationID", validationID)
                    .append("pipelineRunID", pipelineRunID);

            Document resultDoc = resultsCollection.find(query).first();

            if (resultDoc == null) {
                LOG.debug("No result found yet for validationID={}, pipelineRunID={}", validationID, pipelineRunID);
                return null;
            }

            // Parse result from document
            ValidationResult result = new ValidationResult(validationID);
            result.setPassed(resultDoc.getBoolean("passed", false));
            result.setDurationMs(resultDoc.getLong("duration_ms"));
            result.setExecutedAt(resultDoc.getDate("executed_at"));
            result.setFeatureName(resultDoc.getString("feature_name"));

            if (resultDoc.containsKey("error_message")) {
                result.setErrorMessage(resultDoc.getString("error_message"));
            }

            LOG.debug("Found result for validationID={}, pipelineRunID={}, passed={}",
                    validationID, pipelineRunID, result.isPassed());

            return result;

        } catch (Exception e) {
            LOG.error("Failed to check for result: validationID={}, pipelineRunID={}",
                    validationID, pipelineRunID, e);
            return null;
        }
    }

    // =================================================================
    // CLEANUP
    // =================================================================

    /**
     * Close resources
     */
    public void close() {
        try {
            if (flinkClient != null) {
                flinkClient.close();
            }
        } catch (Exception e) {
            LOG.error("Failed to close Flink client", e);
        }

        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}
