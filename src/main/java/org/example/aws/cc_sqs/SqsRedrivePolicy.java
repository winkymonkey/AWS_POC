package org.example.aws.cc_sqs;


class SqsRedrivePolicy {
	private String maxReceiveCount;
	private String deadLetterTargetArn;
	
	public String getMaxReceiveCount() {
		return maxReceiveCount;
	}
	public void setMaxReceiveCount(String maxReceiveCount) {
		this.maxReceiveCount = maxReceiveCount;
	}
	public String getDeadLetterTargetArn() {
		return deadLetterTargetArn;
	}
	public void setDeadLetterTargetArn(String deadLetterTargetArn) {
		this.deadLetterTargetArn = deadLetterTargetArn;
	}
	
	SqsRedrivePolicy(String maxReceiveCount, String deadLetterTargetArn) {
		this.maxReceiveCount = maxReceiveCount;
		this.deadLetterTargetArn = deadLetterTargetArn;
	}
}
