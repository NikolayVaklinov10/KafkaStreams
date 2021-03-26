package com.kafka.clickstream.model;

public class PageView {
    int userID;
    String page;

    public PageView(int userID, String page) {
        this.userID = userID;
        this.page = page;
    }

    public int getUserID() {
        return userID;
    }

    public String getPage() {
        return page;
    }
}
