package io.softwarestrategies.deadlinequeue.data.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class SendReminderRequest {

    private long userId;
    private String reminderSubject;
    private String reminderMessage;
}
