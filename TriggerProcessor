package com.fiserv.fef.jobs.processors;

import com.fiserv.fef.jobs.JobConfig;
import com.fiserv.fef.models.*;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.api.common.JobID;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static com.mongodb.client.model.Filters.eq;

/**
 * Processes validation triggers:
 * 1. Loads validation configs from validation_registry
 * 2. Submits worker jobs for each validation
 * 3. Waits for job completion
 * 4. Aggregates results
 */
public class TriggerProcessor extends ProcessFunction<ValidationTrigger, ProcessValidationResult> {

    private static final Logger LOG = LoggerFactory.getLogger(TriggerProcessor.class);
    private static final long serialVersionUID = 1L;

    private final JobConfig config;

    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> registryCollection;
    private transient MongoCollection<Document> triggersCollection;
    private transient MongoCollection<Document> scoreResultsCollection;
    private transient JobSubmitter jobSubmitter;
    private transient ExecutorService executorService;

    public TriggerProcessor(JobConfig config) {
        this.config = config;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        LOG.info("Opening TriggerProcessor");

        // Initialize MongoDB connection (use FEF database for config collections)
        mongoClient = MongoClients.create(config.getMongoUri());
        MongoDatabase database = mongoClient.getDatabase(config.getMongoFefDatabase());  // Use FEF database
        registryCollection = database.getCollection(config.getRegistryCollection());
        triggersCollection = database.getCollection(config.getPipelineCollection());
        scoreResultsCollection = database.getCollection(config.getScoreCollection());

        LOG.info("MongoDB connection initialized for TriggerProcessor (FEF database: {})", config.getMongoFefDatabase());

        // Initialize job submitter
        jobSubmitter = new JobSubmitter(config);

        // Create thread pool for parallel job submission with configurable size
        int threadPoolSize = config.getWorkerThreadPoolSize();
        LOG.info("Creating worker thread pool with size: {}", threadPoolSize);
        executorService = Executors.newFixedThreadPool(threadPoolSize);

        // Initialize score results collection (create if not exists)
        scoreResultsCollection = database.getCollection("fef_validation_scores");
    }

    @Override
    public void processElement(
            ValidationTrigger trigger,
            Context ctx,
            Collector<ProcessValidationResult> out
    ) throws Exception {

        LOG.info("Processing trigger: process={}, date={}",
                trigger.getProcessID(), trigger.getValidationDate());

        try {
            // Update trigger status to indicate execution has started
            updateExecutionStarted(trigger.getId());

            // Step 1: Load validation configs for this process (with tenant DB names for per-tenant registry support)
            List<ValidationConfig> validationConfigs = loadValidationConfigs(
                    trigger.getProcessID(),
                    trigger.getSourceDbName(),
                    trigger.getTargetDbName()
            );

            if (validationConfigs.isEmpty()) {
                LOG.warn("No validations found for process: {}", trigger.getProcessID());
                return;
            }

            LOG.info("Loaded {} validation configs", validationConfigs.size());

            // Step 2: Submit worker jobs in parallel
            List<Future<ValidationResult>> futures = submitWorkerJobs(
                    validationConfigs,
                    trigger.getValidationDate(),
                    trigger.getId()  // Pass trigger ID as pipelineRunID
                     );

            // Step 3: Wait for all jobs to complete
            List<ValidationResult> results = waitForResults(futures);

            // Step 4: Aggregate results
            ProcessValidationResult processResult = aggregateResults(
                    trigger.getProcessID(),
                    results
            );
            processResult.setPipelineRunID(trigger.getId());  // Set the trigger document ID
            processResult.markComplete();  // sets completedAt + totalDurationMs

            long duration = processResult.getTotalDurationMs();

            // --- FEF Score Calculation ---
            int totalAccounts = processResult.getTotalValidations();
            int successfulAccounts = processResult.getPassedValidations();
            String processName = processResult.getProcessName();
            String processID = processResult.getProcessID();
            double score = totalAccounts > 0 ? ((double) successfulAccounts / totalAccounts) * 100.0 : 0.0;
            String executionDate = java.time.LocalDateTime.now().toString();
            ScoreComponent scoreComponent = new ScoreComponent(
                    "FEF_SCORE",
                    score,
                    String.format("%d success out of %d (%.2f%%)", successfulAccounts, totalAccounts, score),
                    executionDate,
                    processName,
                    processID
            );
            //processResult.setScoreComponents(scoreComponents);
            // --- End FEF Score Calculation ---

            // --- Store FEF Score in MongoDB ---
            try {
                Document scoreDoc = new Document()
                        .append("name", scoreComponent.getName())
                        .append("value", scoreComponent.getValue())
                        .append("description", scoreComponent.getDescription())
                        .append("executionDate", scoreComponent.getExecutionDate())
                        .append("processName", scoreComponent.getProcessName())
                        .append("processID", scoreComponent.getProcessID());
                scoreResultsCollection.insertOne(scoreDoc);
                LOG.info("Stored FEF score in MongoDB for processID={}, pipelineRunID={}", trigger.getProcessID(), trigger.getId());
            } catch (Exception e) {
                LOG.error("Failed to store FEF score in MongoDB for processID={}, pipelineRunID={}", trigger.getProcessID(), trigger.getId(), e);
            }
            // --- End Store FEF Score ---

            LOG.info("Process validation complete: process={}, passed={}/{}, duration={}ms",
                    trigger.getProcessID(),
                    processResult.getPassedValidations(),
                    processResult.getTotalValidations(),
                    duration
            );

            try {
                out.collect(processResult);
            } catch (org.apache.flink.runtime.execution.CancelTaskException e) {
                // Expected during cancellation – do NOT fail the job
                LOG.info(
                        "Skipping emit because task was cancelled: processID={}, pipelineRunID={}",
                        trigger.getProcessID(),
                        trigger.getId()
                );
            }


        } catch (org.apache.flink.runtime.execution.CancelTaskException e) {
            // Normal during shutdown/cancel
            LOG.info(
                    "TriggerProcessor interrupted due to task cancellation: processID={}, pipelineRunID={}",
                    trigger.getProcessID(),
                    trigger.getId()
            );
        }
        catch (Exception e) {
            LOG.error("Failed to process trigger: processID={}, pipelineRunID={}",
                    trigger.getProcessID(), trigger.getId(), e);

            throw new RuntimeException("Failed to process trigger: " + trigger.getProcessID(), e);
        }
    }

    /**
     * Load validation configs from registry
     */
    private List<ValidationConfig> loadValidationConfigs(String processID,String sourceDbName,
                                                         String targetDbName) {
        LOG.debug("Loading validation configs for process: {}, sourceDb: {}, targetDb: {}",
                processID, sourceDbName, targetDbName);
        List<ValidationConfig> configs = new ArrayList<>();

        try {
            // Resolve which registry collection to query:
            // Use the tenant's own database when a distinct sourceDbName is provided.
            MongoCollection<Document> resolvedRegistry = registryCollection; // central default
            boolean usingTenantRegistry = false;

            if (sourceDbName != null && !sourceDbName.isBlank()
                    && !sourceDbName.equals(config.getMongoDatabase())) {
                MongoDatabase tenantDb = mongoClient.getDatabase(sourceDbName);
                MongoCollection<Document> tenantRegistry =
                        tenantDb.getCollection(config.getRegistryCollection());

                // Only use tenant registry if it actually has records for this process
                long tenantCount = tenantRegistry.countDocuments(
                        and(eq("processID", processID), eq("enabled", true)));

                if (tenantCount > 0) {
                    resolvedRegistry = tenantRegistry;
                    usingTenantRegistry = true;
                    LOG.info("Using per-tenant registry from db='{}' ({} configs found) for process='{}'",
                            sourceDbName, tenantCount, processID);
                } else {
                    LOG.info("Tenant registry in db='{}' has no configs for process='{}', " +
                            "falling back to central registry", sourceDbName, processID);
                }
            }

            if (!usingTenantRegistry) {
                LOG.debug("Using central registry for process='{}'", processID);
            }

            List<Document> docs = resolvedRegistry

                    .find(and(
                            eq("processID", processID),
                            eq("enabled", true)
                    ))
                    .into(new ArrayList<>());

            for (Document doc : docs) {
                ValidationConfig vc = convertToValidationConfig(doc);
                // Inject tenant DB names so worker jobs always have the correct databases
                vc.setSourceDbName(sourceDbName);
                vc.setTargetDbName(targetDbName);
                configs.add(vc);
            }
            LOG.info("Loaded {} validation configs for process='{}' (tenantRegistry={})",
                    configs.size(), processID, usingTenantRegistry);

        } catch (Exception e) {
            LOG.error("Failed to load validation configs for process='{}'", processID, e);
        }

        return configs;
    }

    /**
     * Convert MongoDB document to ValidationConfig, setting profileRole for each DataSourceConfig
     */
    private ValidationConfig convertToValidationConfig(Document doc) {
        ValidationConfig config = new ValidationConfig();

        config.setProcessID(doc.getString("processID"));
        config.setProcessName(doc.getString("processName"));
        config.setValidationID(doc.getString("validationID"));
        config.setValName(doc.getString("valName"));
        config.setValDescription(doc.getString("valDescription"));
        config.setTrack(doc.getString("track"));
        config.setOrchestration(doc.getString("orchestration"));
        config.setProcessStepNumber(doc.getInteger("step_number", 0));
        config.setExecutionMode(doc.getString("execution_mode"));
        config.setFeatureID(doc.getString("featureID"));
        config.setEnabled(doc.getBoolean("enabled", true));

        // Distributed parquet config
        Document distConfig = (Document) doc.get("distributed_config");
        if (distConfig != null) {
            config.setDistributedJoinKey(distConfig.getString("join_key"));

            // Handle parallelism stored as either Integer or String (common MongoDB authoring mistake)
            Object parallelismVal = distConfig.get("parallelism");
            if (parallelismVal instanceof Integer) {
                config.setDistributedParallelism((Integer) parallelismVal);
            } else if (parallelismVal instanceof String) {
                try {
                    config.setDistributedParallelism(Integer.parseInt((String) parallelismVal));
                    LOG.warn("distributed_config.parallelism stored as String '{}' for validationID={} — " +
                             "store as integer in MongoDB to avoid this warning",
                             parallelismVal, doc.getString("validationID"));
                } catch (NumberFormatException e) {
                    LOG.error("Invalid distributed_config.parallelism value '{}' for validationID={} — defaulting to 0",
                             parallelismVal, doc.getString("validationID"));
                }
            } else {
                config.setDistributedParallelism(distConfig.getInteger("parallelism", 0));
            }

            config.setDistributedExpectedUpdates(distConfig.getString("expected_updates"));
        }
        // execution_profiles
        Document profilesDoc = (Document) doc.get("execution_profiles");
        if (profilesDoc != null) {
            List<DataSourceConfig> targetDataSources = new ArrayList<>();
            for (Map.Entry<String, Object> entry : profilesDoc.entrySet()) {
                String profileRole = entry.getKey(); // e.g., sourceProfile, targetProfile
                Object value = entry.getValue();
                if (value instanceof Document profileDoc) {
                    DataSourceConfig dataSourceConfig = new DataSourceConfig();
                    dataSourceConfig.setCollection(profileDoc.getString("profileType"));
                    dataSourceConfig.setTopic(profileDoc.getString("profileName"));
                    dataSourceConfig.setProfileRole(profileRole); // set source/target role
                    targetDataSources.add(dataSourceConfig);
                }
            }
            config.setTargetDataSources(targetDataSources);
        }
        return config;
    }

    /**
     * Submit worker jobs in parallel
     */
    private List<Future<ValidationResult>> submitWorkerJobs(
            List<ValidationConfig> configs,
            String validationDate,
            String pipelineRunID ) {
        LOG.info("Submitting {} worker jobs", configs.size());

        List<Future<ValidationResult>> futures = new ArrayList<>();

        for (ValidationConfig config : configs) {
            // sourceDbName / targetDbName are already set on the config by loadValidationConfigs()
            Future<ValidationResult> future = executorService.submit(() -> {
                try {
                    // Route based on execution mode
                    JobID jobId;
                    String executionMode = config.getExecutionMode();

                    LOG.info("Execution Mode: {}", executionMode);

                    if ("distributed_parquet".equalsIgnoreCase(executionMode)) {
                        // Submit distributed parquet comparison job (multi-slot, for 10M+ records)
                        LOG.info("Submitting DISTRIBUTED parquet job: validation={}", config.getValidationID());
                        jobId = jobSubmitter.submitDistributedParquetJob(config, validationDate, pipelineRunID);
                    } else {
                        // Default: Submit standard worker job (Gherkin-based)
                        jobId = jobSubmitter.submitWorkerJob(config, validationDate, pipelineRunID);
                    }

                    LOG.info("Submitted worker job: validation={}, mode={}, jobId={}",
                            config.getValidationID(), executionMode != null ? executionMode : "default", jobId);

                    // Wait for job completion
                    ValidationResult result = jobSubmitter.waitForJobCompletion(jobId, config, pipelineRunID);

                    return result;

                } catch (Exception e) {
                    LOG.error("Failed to submit/execute worker job: {}",
                            config.getValidationID(), e);

                    ValidationResult failureResult = new ValidationResult(config.getValidationID());
                    failureResult.setPassed(false);
                    failureResult.setErrorMessage("Job submission failed: " + e.getMessage());

                    return failureResult;
                }
            });

            futures.add(future);
        }

        return futures;
    }

    /**
     * Wait for all results
     */
    private List<ValidationResult> waitForResults(List<Future<ValidationResult>> futures) {
        LOG.info("Waiting for {} worker jobs to complete", futures.size());

        List<ValidationResult> results = new ArrayList<>();

        for (Future<ValidationResult> future : futures) {
            try {
                // Wait up to 60 minutes per validation (large parquet files need more time)
                long timeoutMinutes = config.getWorkerJobTimeoutMinutes();
                ValidationResult result = future.get(timeoutMinutes, TimeUnit.MINUTES);
                results.add(result);

            } catch (TimeoutException e) {
                LOG.error("Worker job timed out", e);

                ValidationResult timeoutResult = new ValidationResult();
                timeoutResult.setPassed(false);
                timeoutResult.setErrorMessage("Validation timed out after " + config.getWorkerJobTimeoutMinutes() + " minutes");
                results.add(timeoutResult);

            } catch (Exception e) {
                LOG.error("Failed to get worker job result", e);

                ValidationResult errorResult = new ValidationResult();
                errorResult.setPassed(false);
                errorResult.setErrorMessage("Failed to get result: " + e.getMessage());
                results.add(errorResult);
            }
        }

        return results;
    }

    /**
     * Aggregate individual validation results into process result
     */
    private ProcessValidationResult aggregateResults(
            String processID,
            List<ValidationResult> validationResults
    ) {
        ProcessValidationResult processResult = new ProcessValidationResult(processID, processID);

        for (ValidationResult result : validationResults) {
            processResult.addValidationResult(result);
        }

        return processResult;
    }

    /**
     * Helper method for MongoDB filter
     */
    private org.bson.conversions.Bson and(org.bson.conversions.Bson... filters) {
        return com.mongodb.client.model.Filters.and(filters);
    }

    /**
     * Update trigger document with execution_started_at timestamp and status="running"
     */
    private void updateExecutionStarted(String triggerID) {
        if (triggerID == null || triggerID.isEmpty()) {
            LOG.warn("No triggerID provided, cannot update execution_started_at");
            return;
        }

        try {
            Date now = new Date();
            Document query = new Document("_id", new org.bson.types.ObjectId(triggerID));

            triggersCollection.updateOne(
                    query,
                    new Document("$set", new Document()
                            .append("status", "running")
                            .append("execution_started_at", now)
                            .append("updated_at", now)
                    )
            );

            LOG.debug("Updated status=running and execution_started_at for triggerID={}", triggerID);
        } catch (Exception e) {
            LOG.error("Failed to update execution_started_at for triggerID={}", triggerID, e);
        }
    }

    @Override
    public void close() {
        LOG.info("Closing TriggerProcessor");

        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
            }
        }

        if (jobSubmitter != null) {
            jobSubmitter.close();
        }

        if (mongoClient != null) {
            mongoClient.close();
        }
    }
}

