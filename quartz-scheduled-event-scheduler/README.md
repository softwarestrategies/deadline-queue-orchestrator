# Quartz Scheduled Event Scheduler

## ✨ Overview

An example system showing how to use AWS EventBridge Scheduler for "dynamic" ScheduledEvent processing (versus fixed-time).

There are several parts to this:

1. A backend service that has a RESTful endpoint(s) for
- Scheduling events with AWS EventBridge Scheduler per instructions in the request.
- Receiving events from AWS EventBridge Scheduler and processing them.
- For Demo sake, a third endpoint is provided to simulate the final event action desired.
3. An AWS Lambda function that can be triggered by EventBridge Scheduler and push the event payload received by it via HTTP call to another of the backend service's RESTful endpoints where the event can be acted on.
3. An AWS EventBridge Scheduler Rule setup to support jobs being scheduled with a given JSON payload and when the job processing is due it evokes the above-described lambada and passes it that JSON.

## 🔧  AWS Setup in the Cloud

### AWS Lambda
There needs to be a lambda function that can be triggered by EventBridge Scheduler.  The code for this will be found in 
the python file /script/lambdaHandler.py

It is pretty simple. You will want to note its ARN.

### EventBridge Scheduler
 
Create a EventBridge Rule with a target of the above lambda function.  Note its ARN as well.

## 🔧  AWS Setup Locally (or where application will run)

### Environment Variables need to be set
These environment variables need to be set in order for the application to run.  They can either be set externally or using your IDE.

- AWS_REGION  (e.g. "us-east-1")
- AWS_LAMBDA_TARGET_FUNCTION_ARN
- AWS_EVENTBRIDGE_SCHEDULER_EXECUTION_ROLE_ARN

### AWS Credentials need to be set

In your ~/.aws/credentials file, add the following:
```
[default]
aws_access_key_id=AKIA...
aws_secret_access_key=6wOJ...
```

## 🚀 Running & testing the flow locally

Start the backend Spring Boot application.

### Schedule an event

Start by scheduling an event to Send a Reminder.  This will be done via the backend service.

**POST http://localhost:8080/scheduled-events**

```
{
    "topic":"SEND_REMINDER",
    "source": "SourceSystem",
    "destinationUrl": "http://localhost:8080/api/v1/reminders", 
    "env":"DEV",
    "delayUnits": "MINUTES",
    "delayUnitAmount": 5,        
    "data": "{\"userId\":111,\"reminderSubject\":\"Reminder Subject\",\"reminderMessage\":\"Reminder Message\"}"
}
```
### Receive and Process the event

Typically you'd receive the event via the lambda function, which was triggered by EventBridge Scheduler.  Here we'll simulate what would be received by the backend in such a case:

This will be done via a REST call to the backend service, using a different endpoint than the one used to schedule the event.

**POST localhost:8080/api/v1/events**

```
{
    "id": "af3a327b-c499-4502-9c40-49bb6c23e58e",
    "topic": "SEND_REMINDER",
    "source": "SourceSystem",
    "destinationUrl": "http://localhost:8080/api/v1/reminders",
    "env": "DEV",
    "delayUnits": "MINUTES",
    "delayUnitAmount": 5,
    "data": "{\"userId\":111,\"reminderSubject\":\"Reminder Subject\",\"reminderMessage\":\"Reminder Message\"}",
    "created": [ 2025, 12, 14, 7, 10, 16, 180730000 ]
}
```

It should ultimate forward the data portion of the event to the destinationUrl.







