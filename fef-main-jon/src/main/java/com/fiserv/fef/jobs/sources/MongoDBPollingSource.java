package com.fiserv.fef.jobs.sources;

import com.fiserv.fef.models.ValidationTrigger;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Updates.set;

/**
 * Flink source function that polls MongoDB for validation triggers
 *
 * Polling Logic:
 * 1. Query MongoDB for triggers with status="queued"
 * 2. Update status to "picked" (atomic operation)
 * 3. Emit triggers to downstream operators
 * 4. Sleep for polling interval
 * 5. Repeat forever
 */
public class MongoDBPollingSource implements SourceFunction<ValidationTrigger> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBPollingSource.class);

    private static final String STATUS_QUEUED = "queuedLocal";
    private static final String STATUS_PICKED = "picked";

    private final String mongoUri;
    private final String databaseName;
    private final String pipelineCollection;
    private final int pollingIntervalSeconds;
    private final int maxTriggersPerPoll;

    private volatile boolean running = true;

    // Transient fields (not serialized)
    private transient MongoClient mongoClient;
    private transient MongoCollection<Document> triggersCollection;

    public MongoDBPollingSource(String mongoUri, String databaseName, String pipelineCollection, int pollingIntervalSeconds, int maxTriggersPerPoll) {
        this.mongoUri = mongoUri;
        this.databaseName = databaseName;
        this.pipelineCollection = pipelineCollection;
        this.pollingIntervalSeconds = pollingIntervalSeconds;
        this.maxTriggersPerPoll = maxTriggersPerPoll;
    }

    @Override
    public void run(SourceContext<ValidationTrigger> ctx) throws Exception {
        LOG.info("Starting MongoDB polling source: interval={}s", pollingIntervalSeconds);

        // Initialize MongoDB connection
        initializeConnection();

        // Main polling loop
        while (running) {
            try {
                // Poll for new triggers
                List<ValidationTrigger> triggers = pollForTriggers();

                // Emit triggers
                for (ValidationTrigger trigger : triggers) {
                    synchronized (ctx.getCheckpointLock()) {
                        ctx.collect(trigger);
                        LOG.info("Emitted trigger: {}", trigger.getId());
                    }
                }

                // Sleep until next poll
                Thread.sleep(pollingIntervalSeconds * 1000L);

            } catch (InterruptedException e) {
                LOG.warn("Polling interrupted", e);
                Thread.currentThread().interrupt();
                break;

            } catch (Exception e) {
                LOG.error("Error during polling", e);
                // Continue polling despite errors
                Thread.sleep(pollingIntervalSeconds * 1000L);
            }
        }

        LOG.info("MongoDB polling source stopped");
    }

    /**
     * Poll MongoDB for new triggers
     */
    private List<ValidationTrigger> pollForTriggers() {
        List<ValidationTrigger> triggers = new ArrayList<>();

        try {
            LOG.info("Polling MongoDB for triggers with status='{}'", STATUS_QUEUED);
            // Find queued triggers
            List<Document> queuedTriggers = triggersCollection
                    .find(eq("status", STATUS_QUEUED))
                    .limit(maxTriggersPerPoll)  // Configurable limit
                    .into(new ArrayList<>());

            LOG.info("Found {} queued triggers", queuedTriggers.size());

            // Process each trigger
            for (Document triggerDoc : queuedTriggers) {
                // Log the document before update
                LOG.info("[BEFORE UPDATE] Trigger doc: {}", triggerDoc.toJson());
                // Atomically update status to "picked" and set picked_up_at
                Document query = new Document("_id", triggerDoc.getObjectId("_id")).append("status", STATUS_QUEUED);
                LOG.info("[UPDATE QUERY] {}", query.toJson());
                Date now = new Date();
                Document update = new Document()
                        .append("$set", new Document()
                                .append("status", STATUS_PICKED)
                                .append("picked_up_at", now)
                                .append("updated_at", now)
                        );
                Document result = triggersCollection.findOneAndUpdate(
                        and(
                                eq("_id", triggerDoc.getObjectId("_id")),
                                eq("status", STATUS_QUEUED)  // Only if still queued
                        ),
                        update
                );
                // Log the update result
                LOG.info("[UPDATE RESULT] {}", result != null ? result.toJson() : "null");
                // Log the document after update
                if (result != null) {
                    Document afterUpdate = triggersCollection.find(eq("_id", triggerDoc.getObjectId("_id"))).first();
                    LOG.info("[AFTER UPDATE] Trigger doc: {}", afterUpdate != null ? afterUpdate.toJson() : "null");
                    if (afterUpdate != null && !afterUpdate.containsKey("processID")) {
                        LOG.warn("[MISSING FIELD] processID is missing after update for _id={}", triggerDoc.getObjectId("_id"));
                    }
                }
                // If update succeeded, convert to ValidationTrigger
                if (result != null) {
                    ValidationTrigger trigger = convertToTrigger(result);
                    triggers.add(trigger);
                }
            }

            if (!triggers.isEmpty()) {
                LOG.info("Picked {} triggers", triggers.size());
            }

        } catch (Exception e) {
            LOG.error("Failed to poll for triggers", e);
        }

        return triggers;
    }

    /**
     * Convert MongoDB document to ValidationTrigger
     */
    private ValidationTrigger convertToTrigger(Document doc) {
        ValidationTrigger trigger = new ValidationTrigger();

        trigger.setId(doc.getObjectId("_id").toString());
        trigger.setProcessID(doc.getString("processID"));
        // Extract source_dbName and target_dbName from pipeline document
        trigger.setSourceDbName(doc.getString("source_dbName"));
        trigger.setTargetDbName(doc.getString("target_dbName"));

        Object filtersObj = doc.get("filters");
        if (filtersObj instanceof org.bson.Document) {
            org.bson.Document filtersDoc = (org.bson.Document) filtersObj;
            trigger.setFilters(filtersDoc);
            // Extract businessDate from filters and set as validationDate
            if (filtersDoc.containsKey("businessDate")) {
                trigger.setValidationDate(filtersDoc.getString("businessDate"));
            } else {
                trigger.setValidationDate(null);
            }
        } else {
            trigger.setFilters(null);
            trigger.setValidationDate(null);
        }
        trigger.setStatus(doc.getString("status"));
        // Robust handling for created_at (String or Date)
        Object createdAtObj = doc.get("created_at");
        if (createdAtObj instanceof java.util.Date) {
            trigger.setCreatedAt((java.util.Date) createdAtObj);
        } else if (createdAtObj instanceof String) {
            try {
                java.time.Instant instant = java.time.Instant.parse((String) createdAtObj);
                trigger.setCreatedAt(java.util.Date.from(instant));
            } catch (Exception e) {
                trigger.setCreatedAt(null); // fallback if parsing fails
            }
        } else {
            trigger.setCreatedAt(null);
        }
        trigger.setPickedAt(new Date());

        return trigger;
    }

    /**
     * Initialize MongoDB connection
     */
    private void initializeConnection() {
        LOG.info("Initializing MongoDB connection: {}", maskUri(mongoUri));

        mongoClient = MongoClients.create(mongoUri);
        MongoDatabase database = mongoClient.getDatabase(databaseName);
        triggersCollection = database.getCollection(pipelineCollection);

        // Create index on status field for efficient querying
        try {
            triggersCollection.createIndex(new Document("status", 1));
            LOG.info("Ensured index on status field");
        } catch (Exception e) {
            LOG.warn("Failed to create index", e);
        }
    }

    /**
     * Mask sensitive information in URI
     */
    private String maskUri(String uri) {
        if (uri == null) return null;
        return uri.replaceAll("://[^@]+@", "://***:***@");
    }

    @Override
    public void cancel() {
        LOG.info("Cancelling MongoDB polling source");
        running = false;

        // Close MongoDB connection
        if (mongoClient != null) {
            mongoClient.close();
            mongoClient = null;
        }
    }
}
