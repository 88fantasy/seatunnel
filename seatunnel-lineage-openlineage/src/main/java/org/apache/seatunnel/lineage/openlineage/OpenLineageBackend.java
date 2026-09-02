/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.lineage.openlineage;

import org.apache.seatunnel.lineage.LineageBackend;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageEvent;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/** HTTP LineageBackend using the project's fixed Apache HttpClient 4.x transport. */
public final class OpenLineageBackend implements LineageBackend {
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineageBackend.class);

    /**
     * Returns the transport name used to select this backend through the lineage SPI.
     *
     * @return the HTTP transport name
     */
    @Override
    public String getName() {
        return "http";
    }

    /**
     * Sends one event using HTTP on a best-effort basis.
     *
     * <p>Failures are logged and ignored. Lineage delivery must never change the outcome of the
     * data-processing job, so this behaviour is not configurable.
     *
     * @param config immutable lineage configuration
     * @param event event to send
     */
    @Override
    public void emit(LineageConfig config, LineageEvent event) {
        if (!config.enabled()) {
            return;
        }
        try {
            sendWithRetry(config, OpenLineageEventConverter.toJson(event));
        } catch (Throwable failure) {
            LOGGER.warn(
                    "Failed to send OpenLineage event to endpoint {} after {} attempt(s); failure type {}",
                    endpointSummary(config.url()),
                    config.retryTimes() + 1,
                    failure.getClass().getName());
        }
    }

    private void sendWithRetry(LineageConfig config, String payload) throws Exception {
        Exception failure = null;
        for (int attempt = 0; attempt <= config.retryTimes(); attempt++) {
            try {
                sendOnce(config, payload);
                return;
            } catch (Exception e) {
                failure = e;
                if (attempt < config.retryTimes()) {
                    try {
                        Thread.sleep(Math.min(100L, 10L * (attempt + 1)));
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw interrupted;
                    }
                }
            }
        }
        throw failure;
    }

    private static String endpointSummary(String endpoint) {
        if (endpoint == null || endpoint.trim().isEmpty()) {
            return "<missing>";
        }
        try {
            URI uri = new URI(endpoint);
            String scheme = uri.getScheme();
            String host = uri.getHost();
            if (scheme == null || host == null) {
                return "<invalid>";
            }
            return uri.getPort() < 0
                    ? scheme + "://" + host
                    : scheme + "://" + host + ":" + uri.getPort();
        } catch (URISyntaxException e) {
            return "<invalid>";
        }
    }

    private void sendOnce(LineageConfig config, String payload) throws IOException {
        if (config.url() == null || config.url().trim().isEmpty()) {
            throw new IllegalArgumentException("openlineage_url must be configured when enabled");
        }
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(config.timeoutMs())
                        .setConnectionRequestTimeout(config.timeoutMs())
                        .setSocketTimeout(config.timeoutMs())
                        .build();
        HttpPost post = new HttpPost(config.url());
        post.setConfig(requestConfig);
        post.setHeader("Content-Type", ContentType.APPLICATION_JSON.getMimeType());
        if (config.authToken() != null) {
            post.setHeader("Authorization", "Bearer " + config.authToken());
        }
        post.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));
        try (CloseableHttpResponse response = SharedClient.INSTANCE.execute(post)) {
            HttpEntity entity = response.getEntity();
            EntityUtils.consumeQuietly(entity);
            int status = response.getStatusLine().getStatusCode();
            if (status < HttpStatus.SC_OK || status >= HttpStatus.SC_MULTIPLE_CHOICES) {
                throw new IOException("OpenLineage endpoint returned HTTP " + status);
            }
        }
    }

    /**
     * Holds the client shared by every send.
     *
     * <p>Building one per attempt also builds a connection pool and an SSL context per attempt,
     * which on the engine paths that emit lineage costs more than the request itself. Timeouts stay
     * per request through {@link RequestConfig}, so one client can serve every configuration. It is
     * intentionally never closed: it lives as long as the process that reports lineage.
     */
    private static final class SharedClient {
        private static final CloseableHttpClient INSTANCE = HttpClients.createDefault();

        private SharedClient() {}
    }
}
