package com.demo;

import com.demo.config.SourceConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DemoSourceConnector extends SourceConnector {

    private SourceConfig config;

    @Override
    public String version() {
        return "0.0.1";
    }

    @Override
    public void start(Map<String, String> map) {
        config = new SourceConfig(map);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return DemoSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        Map<String, String> taskProps = new HashMap<>(config.originalsStrings());
        List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; ++i) {
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return SourceConfig.conf();
    }
}
