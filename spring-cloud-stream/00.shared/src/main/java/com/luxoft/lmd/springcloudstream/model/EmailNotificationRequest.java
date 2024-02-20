package com.luxoft.lmd.springcloudstream.model;

public record EmailNotificationRequest(
	String recipient,
	String title,
	String content
) {

}
