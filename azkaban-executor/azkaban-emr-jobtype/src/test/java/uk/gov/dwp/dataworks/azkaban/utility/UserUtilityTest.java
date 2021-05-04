package uk.gov.dwp.dataworks.azkaban.utility;

import azkaban.utils.Props;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static azkaban.Constants.JobProperties.USER_TO_PROXY;
import static azkaban.flow.CommonJobProperties.SUBMIT_USER;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EMRStep.AZKABAN_SERVICE_USER;
import static uk.gov.dwp.dataworks.azkaban.jobtype.EMRStep.USE_EMR_USER;

class UserUtilityTest {

    @ParameterizedTest
    @CsvSource({
            "false,'',          user,proxy,proxy",
            "false,'',          user,'',   user",
            "false,'',          user,user, user",
            "true, '',          user,user, user",
            "false,service-user,user,proxy,service-user",
            "false,service-user,'',  '',   service-user",
            "true, service-user,user,proxy,service-user",
            "true, service-user,'',  '',   service-user",
    })
    public void shouldGiveAppropriateEffectiveUser(
            boolean proxyDisallowed,
            String serviceUser,
            String submitUser,
            String proxyUser,
            String expected) {
        Props systemProps = mock(Props.class);
        when(systemProps.getBoolean(USE_EMR_USER, false)).thenReturn(proxyDisallowed);
        when(systemProps.getString(AZKABAN_SERVICE_USER, "")).thenReturn(serviceUser);
        Props jobProps = mock(Props.class);
        when(jobProps.getString(SUBMIT_USER, "")).thenReturn(submitUser);
        when(jobProps.getString(USER_TO_PROXY, "")).thenReturn(proxyUser);
        String effectiveUser = UserUtility.effectiveUser(jobProps, systemProps);
        assertEquals(expected, effectiveUser);
    }

    @ParameterizedTest
    @ValueSource(strings = { "true", "false" })
    public void shouldThrowExceptionIfNoUsersInProperties(boolean proxyDisallowed) {
        Props systemProps = mock(Props.class);
        when(systemProps.getString(AZKABAN_SERVICE_USER, "")).thenReturn("");
        when(systemProps.getBoolean(USE_EMR_USER, false)).thenReturn(proxyDisallowed);
        Props jobProps = mock(Props.class);
        when(jobProps.getString(SUBMIT_USER, "")).thenReturn("");
        when(jobProps.getString(USER_TO_PROXY, "")).thenReturn("");
        RuntimeException exception = assertThrows(RuntimeException.class, () -> UserUtility.effectiveUser(jobProps, systemProps));
        assertEquals("No service user, submit user or proxy user specified.", exception.getMessage());
    }


    @Test
    public void shouldThrowExceptionWhenProxyDisallowedAndProxyDoesNotMatchUser() {
        Props systemProps = mock(Props.class);
        when(systemProps.getString(AZKABAN_SERVICE_USER, "")).thenReturn("");
        when(systemProps.getBoolean(USE_EMR_USER, false)).thenReturn(true);
        Props jobProps = mock(Props.class);
        when(jobProps.getString(SUBMIT_USER, "")).thenReturn("user");
        when(jobProps.getString(USER_TO_PROXY, "")).thenReturn("proxy");
        RuntimeException exception = assertThrows(RuntimeException.class, () -> UserUtility.effectiveUser(jobProps, systemProps));
        assertEquals("'" + USER_TO_PROXY + "' property (proxy) must be the same as '"
                + SUBMIT_USER + "' property (user) when '" + USE_EMR_USER + "' (true) is true.", exception.getMessage());
    }

    @ParameterizedTest
    @CsvSource({
            "false,service-user,service-user,  group,service-user",
            "false,service-user,effective-user,group,service-user",
            "false,'',          effective-user,group,group",
            "true, service-user,effective-user,group,service-user",
            "true, '',          effective-user,group,effective-user",
    })
    public void testEmrUser(boolean proxyDisallowed, String serviceUser, String effectiveUser, String userGroup,
            String expected) {
        Props systemProps = mock(Props.class);
        when(systemProps.getBoolean(USE_EMR_USER, false)).thenReturn(proxyDisallowed);
        when(systemProps.getString(AZKABAN_SERVICE_USER, "")).thenReturn(serviceUser);
        String emrUser = UserUtility.emrUser(systemProps, effectiveUser, x -> userGroup);
        assertEquals(expected, emrUser);
    }
}
