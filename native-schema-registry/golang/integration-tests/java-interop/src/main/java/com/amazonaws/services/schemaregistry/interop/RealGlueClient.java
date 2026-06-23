/*
 * Lazy-initialized AWSSchemaRegistryClient for the Kafka-in-the-loop
 * endpoints. Uses DefaultCredentialsProvider so the sidecar picks up
 * the same AWS_PROFILE / AWS_ACCESS_KEY_ID chain the surrounding
 * integration tests rely on.
 *
 * The region defaults to us-east-2 (matching native-schema-registry's
 * REAL-AWS-RUNBOOK.md). Override via the AWS_REGION env or the request's
 * "region" field if present.
 *
 * Shared single instance per region to keep the SDK's HTTP connection
 * pool warm between successive /kafka-* requests in the same test run.
 */
package com.amazonaws.services.schemaregistry.interop;

import com.amazonaws.services.schemaregistry.common.AWSSchemaRegistryClient;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class RealGlueClient {
    private static final ConcurrentHashMap<String, AWSSchemaRegistryClient> CLIENTS = new ConcurrentHashMap<>();

    private RealGlueClient() {}

    /**
     * Returns a shared AWSSchemaRegistryClient for the given region.
     * Constructed on first call, then cached per region.
     */
    public static AWSSchemaRegistryClient get(String region) {
        String key = region == null ? defaultRegion() : region;
        return CLIENTS.computeIfAbsent(key, RealGlueClient::build);
    }

    private static AWSSchemaRegistryClient build(String region) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, region);
        GlueSchemaRegistryConfiguration cfg = new GlueSchemaRegistryConfiguration(configs);
        return new AWSSchemaRegistryClient(DefaultCredentialsProvider.create(), cfg);
    }

    /**
     * Returns the resolved default region: AWS_REGION env wins, else
     * us-east-2 to match the project's beta canary anchor.
     */
    public static String defaultRegion() {
        String env = System.getenv("AWS_REGION");
        if (env != null && !env.isEmpty()) {
            return env;
        }
        return "us-east-2";
    }
}
