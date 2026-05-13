package com.fiserv.fef.jobs.processors;

import com.fiserv.fef.jobs.JobConfig;
import com.fiserv.fef.models.ProcessValidationResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Collects validation results and writes them to MongoDB
 */
public class ResultCollector implements SinkFunction<ProcessValidationResult> {

    private static final Logger LOG = LoggerFactory.getLogger(ResultCollector.class);


    private final JobConfig config;

    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> resultsCollection;
    private transient MongoCollection<Document> triggersCollection;

    public ResultCollector(JobConfig config) {
        this.config = config;
    }

    @Override
    public void invoke(ProcessValidationResult result, Context context) {
        LOG.info("Collecting result: process={}, passed={}/{}, pipelineRunID={}",
                result.getProcessID(),
                result.getPassedValidations(),
                result.getTotalValidations(),
                result.getPipelineRunID()
        );

        // Initialize connections if needed
        if (mongoClient == null) {
            initializeConnection();
        }

        try {
            // Validate that this is a complete result
            if (result.getTotalValidations() == 0) {
                LOG.warn("Ignoring incomplete result with 0 total validations: process={}, pipelineRunID={}",
                        result.getProcessID(), result.getPipelineRunID());
                return;
            }

            // Write result to MongoDB
            writeResult(result);

            // Update trigger status
            updateTriggerStatus(result);

            LOG.info("Result collected successfully: process={}, pipelineRunID={}",
                    result.getProcessID(), result.getPipelineRunID());

        } catch (Exception e) {
            LOG.error("Failed to collect result: process={}, pipelineRunID={}",
                    result.getProcessID(), result.getPipelineRunID(), e);
        }
    }

    /**
     * Write result to MongoDB
     */
    private void writeResult(ProcessValidationResult result) {
        Document resultDoc = new Document()
                .append("pipelineRunID", result.getPipelineRunID())
                .append("processID", result.getProcessID())
                .append("processName", result.getProcessName())
                .append("total_validations", result.getTotalValidations())
                .append("passed_validations", result.getPassedValidations())
                .append("failed_validations", result.getFailedValidations())
                .append("all_passed", result.allPassed())
                .append("started_at", result.getStartedAt())
                .append("completed_at", result.getCompletedAt())
                .append("total_duration_ms", result.getTotalDurationMs())
                .append("created_at", new Date());

        resultsCollection.insertOne(resultDoc);

        LOG.debug("Result written to MongoDB: {}", result.getProcessID());
    }

    /**
     * Update trigger status to completed/failed
     */
    private void updateTriggerStatus(ProcessValidationResult result) {
        if (result.getPipelineRunID() == null || result.getPipelineRunID().isEmpty()) {
            LOG.warn("No pipelineRunID in result, cannot update trigger status");
            return;
        }

        String status = result.allPassed() ? "completed" : "failed";

        // Query by _id to update the specific trigger document
        Document query = new Document("_id", new org.bson.types.ObjectId(result.getPipelineRunID()));

        // Log the document before update
        Document beforeUpdate = triggersCollection.find(query).first();
        LOG.info("[BEFORE UPDATE] Trigger doc: {}", beforeUpdate != null ? beforeUpdate.toJson() : "null");
        LOG.info("[UPDATE] Setting status to '{}' for pipelineRunID={}, validations: {}/{} passed",
                status, result.getPipelineRunID(),
                result.getPassedValidations(), result.getTotalValidations());

        Date now = new Date();
        triggersCollection.updateOne(
                query,
                new Document("$set", new Document()
                        .append("status", status)
                        .append("completed_at", now)
                        .append("updated_at", now)
                        .append("result_summary", String.format(
                                "Passed: %d/%d, Duration: %dms",
                                result.getPassedValidations(),
                                result.getTotalValidations(),
                                result.getTotalDurationMs()
                        ))
                )
        );

        // Log the document after update
        Document afterUpdate = triggersCollection.find(query).first();
        LOG.info("[AFTER UPDATE] Trigger doc: {}", afterUpdate != null ? afterUpdate.toJson() : "null");
        LOG.debug("Trigger status updated: processID={}, pipelineRunID={}, status={}",
                result.getProcessID(), result.getPipelineRunID(), status);
    }

    /**
     * Initialize MongoDB connection (uses FEF database for results and pipeline collections)
     */
    private void initializeConnection() {
        mongoClient = MongoClients.create(config.getMongoUri());
        MongoDatabase database = mongoClient.getDatabase(config.getMongoFefDatabase());  // Use FEF database
        resultsCollection = database.getCollection(config.getResultsCollection());
        triggersCollection = database.getCollection(config.getPipelineCollection());

        LOG.info("MongoDB connection initialized for ResultCollector (FEF database: {})", config.getMongoFefDatabase());
    }
}
