package com.demo;

import com.demo.config.SourceConfig;
import com.demo.constant.TaskConstants;
import com.demo.service.RequestService;
import com.demo.service.impl.RequestServiceImpl;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.lang.System.currentTimeMillis;

public class DemoSourceTask extends SourceTask {

    private static final Logger LOG = LoggerFactory.getLogger(DemoSourceTask.class);
    private Long lastPollTime = 0L;
    private Long pollInterval;
    private String url;
    private List<String> headers;
    private String topic;

    @Override
    public void start(Map<String, String> configs) {
        SourceConfig connectorConfig = new SourceConfig(configs);
        url = connectorConfig.getUrl();
        headers = connectorConfig.getRequestHeaders();
        topic = connectorConfig.getTopic();
        pollInterval = connectorConfig.getPollInterval();
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        long millis = pollInterval - (currentTimeMillis() - lastPollTime);
        if (millis > 0) {
            Thread.sleep(millis);
        }
        try {
            LOG.debug("request sent to: {}", url);
            RequestService requestService = new RequestServiceImpl();
            String response = requestService.execute(url, headers);
            LOG.debug("response: {}", response);
            return formSourceRecords(response);
        } catch (Exception e) {
            LOG.error("HTTP call execution failed " + e.getMessage(), e);
            return Collections.emptyList();
        } finally {
            lastPollTime = currentTimeMillis();
        }
    }

    private List<SourceRecord> formSourceRecords(String response) {
        List<String> rawRecords = new ArrayList<>();
        JSONArray jRecords = new JSONArray(response);
        if (jRecords.isEmpty()) {
            return Collections.emptyList();
        } else {
            for (int i = 0; i < jRecords.length(); i++) {
                rawRecords.add(jRecords.getJSONObject(i).toString());
            }
        }
        Map<String, String> sourcePartition = Collections.singletonMap(TaskConstants.URL, url);
        Map<String, Long> sourceOffset = Collections.singletonMap(TaskConstants.TIMESTAMP, currentTimeMillis());
        return rawRecords.stream()
                .map(r -> formSourceRecord(sourcePartition, sourceOffset, r))
                .collect(Collectors.toList());
    }

    private SourceRecord formSourceRecord(Map<String, String> sourcePartition, Map<String, Long> sourceOffset, String value) {
        return new SourceRecord(sourcePartition, sourceOffset,
                topic,
                Schema.STRING_SCHEMA, value);
    }


    @Override
    public void stop() {
        LOG.debug("Stopping source constant");
    }

    @Override
    public String version() {
        return "0.0.1";
    }
}
