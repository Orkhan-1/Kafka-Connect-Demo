package com.demo.service.impl;

import com.demo.service.RequestService;
import okhttp3.ConnectionPool;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class RequestServiceImpl implements RequestService {

    private static Logger LOG = LoggerFactory.getLogger(RequestServiceImpl.class);

    private final OkHttpClient client;

    public RequestServiceImpl() {
        client = new OkHttpClient.Builder()
                .connectionPool(new ConnectionPool(3, 300, TimeUnit.SECONDS))
                .connectTimeout(3, TimeUnit.SECONDS)
                .readTimeout(3, TimeUnit.SECONDS)
                .build();
    }

    @Override
    public String execute(String url, List<String> rawHeaders) {

        Map<String, String> headers = rawHeaders.stream()
                .map(a -> a.split(":", 2))
                .collect(Collectors.toMap(a -> a[0], a -> a[1]));
        LOG.debug("header size {}", headers.size());
        String[] tempHeaders = headers.entrySet()
                .stream()
                .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
                .toArray(String[]::new);

        okhttp3.Request.Builder builder = new okhttp3.Request.Builder()
                .url(url)
                .headers(Headers.of(tempHeaders));
        builder.get().url(url);
        LOG.debug("url {} was formed", url);
        okhttp3.Request okRequest = builder.build();
        try (okhttp3.Response okResponse = client.newCall(okRequest).execute()) {
            return okResponse.body() != null ? okResponse.body().string() : null;
        } catch (IOException e) {
            throw new RetriableException(e.getMessage(), e);
        }
    }
}
