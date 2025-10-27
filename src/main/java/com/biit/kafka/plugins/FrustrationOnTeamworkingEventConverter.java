package com.biit.kafka.plugins;

/*-
 * #%L
 * Frustration on Teamworking Organization Statistics Generator
 * %%
 * Copyright (C) 2022 - 2025 BiiT Sourcing Solutions S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

import com.biit.drools.form.DroolsForm;
import com.biit.kafka.events.Event;
import com.biit.kafka.events.EventCustomProperties;
import com.biit.kafka.events.EventSubject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Component
public class FrustrationOnTeamworkingEventConverter {

    private static final String DROOLS_RESULT_EVENT_TYPE = "DroolsResultForm";
    public static final String FORM_ORGANIZATION_OUTPUT = "Frustration On Teamworking Organization";
    public static final String FORM_TEAM_OUTPUT = "Frustration On Teamworking Team";

    @Value("${spring.application.name:#{null}}")
    private String applicationName;

    public Event getEvent(DroolsForm droolsForm, String createdBy, UUID sessionId) {
        final Event event = new Event(droolsForm.getDroolsSubmittedForm());
        event.setCreatedBy(createdBy);
        event.setMessageId(UUID.randomUUID());
        event.setSubject(EventSubject.CREATED.toString());
        event.setContentType(MediaType.APPLICATION_JSON_VALUE);
        event.setCreatedAt(LocalDateTime.now());
        event.setReplyTo(applicationName);
        event.setSessionId(sessionId);
        event.setTag(droolsForm.getTag());
        event.setCustomProperty(EventCustomProperties.FACT_TYPE, DROOLS_RESULT_EVENT_TYPE);
        event.setCustomProperty(EventCustomProperties.SOURCE_TAG, FORM_ORGANIZATION_OUTPUT);
        return event;
    }
}
