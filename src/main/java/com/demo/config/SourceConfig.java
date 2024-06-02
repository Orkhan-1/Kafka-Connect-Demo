package com.demo.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

public class SourceConfig extends AbstractConfig {

    public static final String API_URL_CONFIG = "api.url";
    private static final String API_URL_DOC = "The URL of an API";
    private static final String API_URL_DISPLAY = "The URL of an API";

    public static final String API_HTTP_METHOD_CONFIG = "api.http.method";
    private static final String API_HTTP_METHOD_DOC = "HTTP method for an API";
    private static final String API_HTTP_METHOD_DISPLAY = "HTTP method for an API";
    private static final String API_HTTP_METHOD_DEFAULT = "GET";

    public static final String API_HTTP_HEADERS_LIST_CONFIG = "api.http.headers";
    private static final String API_HTTP_HEADERS_LIST_DISPLAY = "HTTP headers for an API";
    private static final String API_HTTP_HEADERS_LIST_DOC = "HTTP headers for an API";

    public static final String API_REQUEST_INTERVAL_CONFIG = "api.request.interval.ms";
    private static final String API_REQUEST_INTERVAL_DOC = "Request interval for URL";
    private static final String API_REQUEST_INTERVAL_DISPLAY = "Request interval for URL";
    private static final Long API_REQUEST_INTERVAL_DEFAULT = 10000L;

    public static final String TOPIC_CONFIG = "demo.topic";
    private static final String TOPIC_DOC = "kafka topic";
    private static final String TOPIC_DISPLAY = "Kafka topic";

    private static final String API_REQUEST_EXECUTOR_CONFIG = "http.executor.class";
    private static final String API_REQUEST_EXECUTOR_DISPLAY = "HTTP request service";
    private static final String API_REQUEST_EXECUTOR_DOC = "HTTP request service. Default implementation is RequestServiceImpl";
    private static final String API_REQUEST_EXECUTOR_DEFAULT = "com.demo.service.impl.RequestServiceImpl";

    @SuppressWarnings("unchecked")
    protected SourceConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public SourceConfig(Map<String, String> parsedConfig) {
        this(conf(), parsedConfig);
    }

    public static ConfigDef conf() {
        String group = "REST";
        int orderInGroup = 0;
        return new ConfigDef()
                .define(API_REQUEST_INTERVAL_CONFIG,
                        Type.LONG,
                        API_REQUEST_INTERVAL_DEFAULT,
                        Importance.LOW,
                        API_REQUEST_INTERVAL_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        API_REQUEST_INTERVAL_DISPLAY)

                .define(API_HTTP_METHOD_CONFIG,
                        Type.STRING,
                        API_HTTP_METHOD_DEFAULT,
                        Importance.HIGH,
                        API_HTTP_METHOD_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        API_HTTP_METHOD_DISPLAY)

                .define(API_HTTP_HEADERS_LIST_CONFIG,
                        Type.LIST,
                        Collections.EMPTY_LIST,
                        Importance.HIGH,
                        API_HTTP_HEADERS_LIST_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        API_HTTP_HEADERS_LIST_DISPLAY)

                .define(API_URL_CONFIG,
                        Type.STRING,
                        NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        API_URL_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        API_URL_DISPLAY)

                .define(TOPIC_CONFIG,
                        Type.STRING,
                        NO_DEFAULT_VALUE,
                        Importance.HIGH,
                        TOPIC_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.SHORT,
                        TOPIC_DISPLAY)

                .define(API_REQUEST_EXECUTOR_CONFIG,
                        Type.CLASS,
                        API_REQUEST_EXECUTOR_DEFAULT,
                        Importance.LOW,
                        API_REQUEST_EXECUTOR_DOC,
                        group,
                        ++orderInGroup,
                        ConfigDef.Width.NONE,
                        API_REQUEST_EXECUTOR_DISPLAY);
    }

    public String getUrl() {
        return this.getString(API_URL_CONFIG);
    }

    public String getTopic() {
        return this.getString(TOPIC_CONFIG);
    }

    public List<String> getRequestHeaders() {
        return this.getList(API_HTTP_HEADERS_LIST_CONFIG);
    }

    public long getPollInterval() {
        return this.getLong(API_REQUEST_INTERVAL_CONFIG);
    }
}
