package com.fiserv.fef.jobs;

import com.fiserv.fef.jobs.processors.ResultCollector;
import com.fiserv.fef.jobs.processors.TriggerProcessor;
import com.fiserv.fef.jobs.sources.MongoDBPollingSource;
import com.fiserv.fef.models.ProcessValidationResult;
import com.fiserv.fef.models.ValidationTrigger;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Flink job that runs 24/7 to orchestrate validations
 *
 * Architecture:
 * 1. Polls MongoDB for validation triggers (every 10 seconds)
 * 2. Processes triggers and loads validation configs
 * 3. Submits worker jobs for each validation
 * 4. Collects results and updates MongoDB
 *
 * This job never exits - it runs continuously
 */
public class MainValidationJob {

    private static final Logger LOG = LoggerFactory.getLogger(MainValidationJob.class);

    public static void main(String[] args) throws Exception {
        LOG.info("=================================================");
        LOG.info("Starting FEF Main Validation Job");
        LOG.info("=================================================");

        // Parse arguments
        JobConfig config = parseArguments(args);

        // Create Flink streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure environment
        configureEnvironment(env, config);

        // Build job pipeline
        buildPipeline(env, config);

        // Execute job (runs forever)
        LOG.info("Starting main job execution...");
        env.execute("FEF Main Validation Job");
    }

    /**
     * Build the main job pipeline
     *
     * Parallelism Strategy:
     * 1. MongoDB Polling Source: parallelism=1 (avoid duplicate polling)
     * 2. Trigger Processor: parallelism=maxConcurrentPipelines (process multiple pipelines simultaneously)
     * 3. Result Collector: parallelism=1 (maintain result ordering)
     */
    private static void buildPipeline(StreamExecutionEnvironment env, JobConfig config) {
        LOG.info("Building main job pipeline");
        LOG.info("Pipeline configuration: maxTriggersPerPoll={}, maxConcurrentPipelines={}, workerThreadPoolSize={}",
                config.getMaxTriggersPerPoll(), config.getMaxConcurrentPipelines(), config.getWorkerThreadPoolSize());

        // Step 1: Create polling source (uses FEF database)
        MongoDBPollingSource pollingSource = new MongoDBPollingSource(
                config.getMongoUri(),
                config.getMongoFefDatabase(),  // Use FEF database for pipeline collection
                config.getPipelineCollection(),
                config.getPollingIntervalSeconds(),
                config.getMaxTriggersPerPoll()
        );

        // Step 2: Create trigger stream with parallelism=1
        // Single source instance to avoid duplicate polling of MongoDB
        DataStream<ValidationTrigger> triggerStream = env
                .addSource(pollingSource)
                .name("MongoDB Polling Source")
                .uid("mongodb-polling-source")
                .setParallelism(1);

        // Step 3: Process triggers (load configs, submit jobs) with parallelism=maxConcurrentPipelines
        // Multiple processor instances to handle different pipelines concurrently
        // Example: If maxConcurrentPipelines=5, Flink creates 5 TriggerProcessor instances
        // Each instance processes a different pipeline (different processID/date combination)
        DataStream<ProcessValidationResult> resultStream = triggerStream
                .process(new TriggerProcessor(config))
                .name("Trigger Processor")
                .uid("trigger-processor")
                .setParallelism(config.getMaxConcurrentPipelines());

        // Step 4: Collect and aggregate results with parallelism=1
        // Single sink to maintain result ordering and avoid race conditions
        resultStream
                .addSink(new ResultCollector(config))
                .name("Result Collector")
                .uid("result-collector")
                .setParallelism(1);

        LOG.info("Pipeline built successfully");
    }

    /**
     * Configure Flink environment
     */
    private static void configureEnvironment(StreamExecutionEnvironment env, JobConfig config) {
        LOG.info("Configuring Flink environment");

        // Set default parallelism for Flink environment
        // This is the number of parallel task slots Flink will allocate
        // Individual operators can override this with their own parallelism settings
        env.setParallelism(config.getFlinkDefaultParallelism());

        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(config.getCheckpointIntervalMs());

        // Set restart strategy
        env.setRestartStrategy(
                org.apache.flink.api.common.restartstrategy.RestartStrategies
                        .fixedDelayRestart(
                                Integer.MAX_VALUE,  // Unlimited restarts
                                org.apache.flink.api.common.time.Time.seconds(10)
                        )
        );

        LOG.info("Environment configured: flinkDefaultParallelism={}, checkpointing={}ms",
                config.getFlinkDefaultParallelism(), config.getCheckpointIntervalMs());
    }

    /**
     * Parse command line arguments
     */
    private static JobConfig parseArguments(String[] args) {
        JobConfig config = new JobConfig();

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mongo-uri":
                    config.setMongoUri(args[++i]);
                    break;
                case "--mongo-database":
                    config.setMongoDatabase(args[++i]);
                    break;
                case "--polling-interval":
                    config.setPollingIntervalSeconds(Integer.parseInt(args[++i]));
                    break;
                case "--checkpoint-interval":
                    config.setCheckpointIntervalMs(Long.parseLong(args[++i]));
                    break;
                case "--flink-jobmanager":
                    config.setFlinkJobManager(args[++i]);
                    break;
                default:
                    LOG.warn("Unknown argument: {}", args[i]);
            }
        }

        // Load from environment if not provided
        if (config.getMongoUri() == null) {
            config.setMongoUri(System.getenv().getOrDefault(
                    "MONGODB_URI", "mongodb://localhost:27017"
            ));
        }

        if (config.getMongoDatabase() == null) {
            config.setMongoDatabase(System.getenv().getOrDefault(
                    "MONGODB_DATABASE", "fef"
            ));
        }

        // Do NOT override JobConfig's resolved flink.jobmanager.address with localhost.
        // JobConfig already resolves from application.properties/env and has OpenShift-friendly defaults.
        if (config.getFlinkJobManager() == null || config.getFlinkJobManager().trim().isEmpty()) {
            config.setFlinkJobManager(System.getenv().getOrDefault(
                    "FLINK_JOBMANAGER", "localhost:8081"
            ));
        }

        LOG.info("Job configuration: {}", config);
        return config;
    }
}
