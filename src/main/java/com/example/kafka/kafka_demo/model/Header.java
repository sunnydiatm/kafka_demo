package com.example.kafka.kafka_demo.model;


public class Header {
    private String correlationid;
    private String subject;
    private String version;

    public String getCorrelationid() {
        return correlationid;
    }

    public String getSubject() {
        return subject;
    }

    public String getVersion() {
        return version;
    }

    public void setCorrelationid(String correlationid) {
        this.correlationid = correlationid;
    }

    public void setSubject(String subject) {
        this.subject = subject;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    @Override
    public String toString() {
        return "Header{" +
                "correlationid='" + correlationid + '\'' +
                ", subject='" + subject + '\'' +
                ", version='" + version + '\'' +
                '}';
    }
}
