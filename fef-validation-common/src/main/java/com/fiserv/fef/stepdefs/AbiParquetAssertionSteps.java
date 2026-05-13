package com.fiserv.fef.stepdefs;

import com.fiserv.fef.gherkin.ValidationContext;
import com.fiserv.fef.gherkin.annotations.Then;
import com.fiserv.fef.models.Dataset;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assertions specific to ABI log and parquet reconciliation.
 */
public class AbiParquetAssertionSteps {

    private static final Logger LOG = LoggerFactory.getLogger(AbiParquetAssertionSteps.class);

    @Then("ABI field {string} should equal parquet row count for {string}")
    public void abiFieldShouldEqualParquetRowCountFor(String abiFieldName, String parquetContext) {
        ValidationContext ctx = ValidationContext.getCurrent();

        Dataset source = ctx.getDataset("source");
        if (source == null || source.isEmpty()) {
            throw new IllegalStateException(
                    "Source ABI dataset is empty or missing. Ensure ABI retrieve step populated context 'source'.");
        }

        Document first = source.getDocuments().get(0);
        if (first == null || !first.containsKey(abiFieldName)) {
            throw new IllegalStateException(
                    "ABI field not found in source dataset: '" + abiFieldName + "'");
        }

        Object abiRaw = first.get(abiFieldName);
        Long abiCount = toLongStrict(abiRaw);
        if (abiCount == null) {
            throw new IllegalStateException(
                    "ABI field '" + abiFieldName + "' is not numeric: " + abiRaw);
        }

        Number parquetCountNum = (Number) ctx.getVariable(parquetContext + "_count");
        if (parquetCountNum == null) {
            throw new IllegalStateException(
                    "Parquet count not found for context '" + parquetContext + "'. Expected variable '" + parquetContext + "_count'.");
        }

        long parquetCount = parquetCountNum.longValue();
        if (abiCount.longValue() != parquetCount) {
            throw new AssertionError(
                    "Count mismatch: ABI field '" + abiFieldName + "'=" + abiCount +
                            " but " + parquetContext + "_count=" + parquetCount);
        }

        LOG.info("Assertion passed: ABI {}={} equals {}_count={}", abiFieldName, abiCount, parquetContext, parquetCount);
    }

    private Long toLongStrict(Object value) {
        if (value == null) return null;
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String s) {
            try {
                return Long.parseLong(s.trim());
            } catch (NumberFormatException ignore) {
                return null;
            }
        }
        return null;
    }
}

