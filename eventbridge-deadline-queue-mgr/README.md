# Eventbridge Scheduled Event Scheduler

## ✨ Overview

An example system showing how to use Quartz Scheduler for "dynamic" ScheduledEvent processing (versus fixed-time).

There are several parts to this:

1. A backend service that has a RESTful endpoint for
- Scheduling events with Quartz Scheduler per instructions in the request.
- For Demo sake, a third endpoint is provided to simulate the final event action desired.
2. A datasource that can be used to store the scheduled events.

## 🔧  Configuration

The following configuration can be used, in the application.properties file

### H2 Database Configuration
The datasource for the sample exercise is an in-memory H2 database.
```
# Database Configuration (Using H2 for this example)
spring.datasource.url=jdbc:h2:mem:eventsdb
spring.datasource.driverClassName=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=password
spring.jpa.database-platform=org.hibernate.dialect.H2Dialect
spring.jpa.hibernate.ddl-auto=update
```

### Quartz Scheduler Configuration
```
# Quartz Configuration: CRITICAL for persistence
spring.quartz.job-store-type=jdbc
spring.quartz.jdbc.initialize-schema=always
# Set the instance name for clustering/identification
spring.quartz.properties.org.quartz.scheduler.instanceName=EventSchedulerInstance
# Set up the thread pool size for concurrent job execution
spring.quartz.properties.org.quartz.threadPool.threadCount=10
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

Eventually, the scheduled job executes and sends the data port of the initial request to the destinationUrl.







