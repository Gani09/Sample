package com.fiserv.fef.datasources;

import com.fiserv.fef.models.DataQuery;
import com.fiserv.fef.models.PerformanceConfig;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fiserv.fef.gherkin.ValidationContext;
import com.fiserv.fef.utils.MongoDBClientFactory;
import com.mongodb.client.MongoDatabase;


/**
 * Factory for creating optimal data sources based on query characteristics
 * Implements smart strategy selection for best performance
 */
public class DataAccessFactory {

    private static final Logger LOG = LoggerFactory.getLogger(DataAccessFactory.class);

    // Thresholds for strategy selection
    private static final long SMALL_DATASET_THRESHOLD = 10_000;
    private static final long MEDIUM_DATASET_THRESHOLD = 100_000;
    private static final long LARGE_DATASET_THRESHOLD = 1_000_000;
    static MongoDatabase multiTenantDatabase() {
        return resolveDatabase(null);
    }
    static MongoDatabase resolveDatabase(String contextName) {

        ValidationContext ctx = ValidationContext.getCurrent();
        if (ctx != null && "target".equalsIgnoreCase(contextName) && ctx.getTargetTenantDatabase() != null) {
            LOG.debug("resolveDatabase('target') -> targetTenantDatabase: {}", ctx.getTargetTenantDatabase().getName());
            return ctx.getTargetTenantDatabase();
        }
        if (ctx != null && ctx.getSrcTenantDatabase() != null) {
            LOG.debug("resolveDatabase('{}') -> srcTenantDatabase: {}", contextName, ctx.getSrcTenantDatabase().getName());
            return ctx.getSrcTenantDatabase();
        }
        if (ctx != null && ctx.getTargetTenantDatabase() != null) {
            LOG.debug("resolveDatabase('{}') -> targetTenantDatabase (fallback): {}", contextName, ctx.getTargetTenantDatabase().getName());
            return ctx.getTargetTenantDatabase();
        }
        return MongoDBClientFactory.getDatabase(); // primary: Optis_fef_config_db
    }

    /**
     * Get optimal data source for query
     * Automatically selects best strategy based on query analysis
     */
    public static DataSource getDataSource(DataQuery query) {
        return getDataSource(query, null, null, null); }

    /**
     * Get optimal data source with performance configuration
     */
    public static DataSource getDataSource(DataQuery query, PerformanceConfig perfConfig) {
        return getDataSource(query, perfConfig, null, null);
    }

    /**
     * Get optimal data source with Flink environment (for distributed processing)
     */
    public static DataSource getDataSource(
            DataQuery query,
            PerformanceConfig perfConfig,
            ExecutionEnvironment flinkEnv
    ) {
        return getDataSource(query, perfConfig, flinkEnv, null);
    }

    /**
     * Get optimal data source with all options including Gherkin context name for DB routing
     */
    public static DataSource getDataSource(
            DataQuery query,
            PerformanceConfig perfConfig,
            ExecutionEnvironment flinkEnv,
            String contextName
    ) {

        // Check for explicit strategy override
        if (perfConfig != null && perfConfig.getStrategy() != null) {
            String strategy = perfConfig.getStrategy();

            if (!"auto".equals(strategy)) {
                LOG.info("Using explicit strategy: {}", strategy);
                return createDataSource(strategy, perfConfig, flinkEnv, contextName);
            }
        }

        // Automatic strategy selection
        String selectedStrategy = selectStrategy(query, perfConfig, flinkEnv);
        LOG.info("Auto-selected strategy: {} for collection: {}", selectedStrategy, query.getCollection());

        return createDataSource(selectedStrategy, perfConfig, flinkEnv, contextName);
    }

    /**
     * Select optimal strategy based on query characteristics
     */
    private static String selectStrategy(DataQuery query, PerformanceConfig perfConfig, ExecutionEnvironment flinkEnv) {
        // Priority 1: Aggregation operations
        if (query.isRequiresAggregation()) {
            LOG.debug("Query requires aggregation -> mongodb_aggregation");
            return "aggregation";
        }

        // Priority 2: Estimate data size
        long estimatedSize = QueryAnalyzer.estimateSize(query);
        LOG.debug("Estimated query size: {}", estimatedSize);

        // Priority 3: Check if join is required
        boolean requiresJoin = query.isRequiresJoin();

        // Decision logic
        if (estimatedSize < SMALL_DATASET_THRESHOLD) {
            // Small dataset: Use cursor (simple and fast)
            LOG.debug("Small dataset ({}) -> cursor", estimatedSize);
            return "cursor";
        } else if (estimatedSize < MEDIUM_DATASET_THRESHOLD) {
            // Small dataset: Use cursor (simple and fast)
            LOG.debug("Medium dataset ({}) -> cursor", estimatedSize);
            return "cursor";
        }
        else if (requiresJoin && flinkEnv != null) {
            // Large dataset with join: Use Flink for distributed join
            LOG.debug("Large dataset ({}) with join -> flink_dataset", estimatedSize);
            return "flink_dataset";
        }
        else if (estimatedSize > LARGE_DATASET_THRESHOLD && flinkEnv != null) {
            // Very large dataset: Use Flink for distributed processing
            LOG.debug("Very large dataset ({}) -> flink_dataset", estimatedSize);
            return "flink_dataset";
        }
        else {
            // Default: Cursor with batching
            LOG.debug("Default -> cursor");
            return "cursor";
        }
    }

    private static DataSource createDataSource(String strategy, PerformanceConfig perfConfig,
                                               ExecutionEnvironment flinkEnv, String contextName) {
        MongoDatabase db = resolveDatabase(contextName);

        switch (strategy.toLowerCase()) {
            case "aggregation":
            case "mongodb_aggregation":
                return new MongoAggregationSource(db);

            case "cursor":
            case "mongodb_cursor":
            case "cursor_batch":
                int batchSize = (perfConfig != null && perfConfig.getBatchSize() != null)
                        ? perfConfig.getBatchSize()
                        : 10000;
                return new MongoCursorSource(db, batchSize);

            case "flink_dataset":
            case "distributed":
                if (flinkEnv == null) {
                    LOG.warn("Flink environment not available, falling back to cursor");
                    return new MongoCursorSource(db);
                }
                int parallelism = (perfConfig != null && perfConfig.getParallelism() != null)
                        ? perfConfig.getParallelism()
                        : flinkEnv.getParallelism();
                return new FlinkDataSetSource(flinkEnv, parallelism);

            default:
                LOG.warn("Unknown strategy: {}, using cursor", strategy);
                return new MongoCursorSource(db);
        }
    }

    /**
     * Get aggregation source explicitly
     */
    public static DataSource getAggregationSource() {
        return new MongoAggregationSource(resolveDatabase(null));
    }
    public static DataSource getAggregationSource(String contextName) {
        return new MongoAggregationSource(resolveDatabase(contextName));
    }

    public static DataSource getCursorSource() {
        return new MongoCursorSource(resolveDatabase(null));

    }

    /**
     * Get cursor source with custom batch size
     */
    public static DataSource getCursorSource(int batchSize) {
        return new MongoCursorSource(resolveDatabase(null), batchSize);
    }

    /**
     * Get Flink DataSet source explicitly
     */
    public static DataSource getFlinkDataSetSource(ExecutionEnvironment env) {
        return new FlinkDataSetSource(env);
    }

    /**
     * Get Flink DataSet source with custom parallelism
     */
    public static DataSource getFlinkDataSetSource(ExecutionEnvironment env, int parallelism) {
        return new FlinkDataSetSource(env, parallelism);
    }
}
