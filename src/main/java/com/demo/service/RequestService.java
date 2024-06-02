package com.demo.service;

import java.util.List;

public interface RequestService {

    String execute(String url, List<String> rawHeaders);
}
