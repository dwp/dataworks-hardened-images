package uk.gov.dwp.dataworks.azkaban.authentication;

import azkaban.user.User;
import azkaban.utils.Props;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.gov.dwp.dataworks.azkaban.authentication.CognitoUserManager.DISALLOW_PROXY_USER;
import static uk.gov.dwp.dataworks.azkaban.authentication.CognitoUserManager.XML_FILE_PARAM;

class CognitoUserManagerTest {

    @ParameterizedTest
    @CsvSource({
            "false,proxy,user,true",
            "false,non-configured-proxy,user,false",
            "false,non-configured-proxy,non-configured-user,false",
            "true,proxy,user,false",
            "true,user,user,true",
            "true,non-configured-user,non-configured-user,true",
    })
    void shouldValidateProxyUsersWhenDisallowProxyFeatureToggledOn(boolean proxyDisallowed, String proxyUser,
            String user, boolean expected) {
        Props props = mock(Props.class);
        when(props.getString(XML_FILE_PARAM)).thenReturn("src/test/resources/azkaban-users-tests.xml");
        when(props.getBoolean(DISALLOW_PROXY_USER, false)).thenReturn(proxyDisallowed);
        CognitoUserManager userManager = new CognitoUserManager(props);
        assertEquals(expected, userManager.validateProxyUser(proxyUser, new User(user)));
    }

    @ParameterizedTest
    @CsvSource({
            "proxy,               user,               true",
            "non-configured-proxy,user,               false",
            "non-configured-proxy,non-configured-user,false",
            "proxy,               user,               true",
            // The following 2 cases represent behaviour prior to 'disallow.proxy.user' introduction,
            // i.e. can't specify launching user as proxy unless it's in the user config.
            "user,                user,               false",
            "non-configured-user, non-configured-user,false",
    })
    void shouldValidateProxyUsersWhenDisallowProxyFeatureToggledOff(String proxyUser, String user, boolean expected) {
        Props props = mock(Props.class);
        when(props.getString(XML_FILE_PARAM)).thenReturn("src/test/resources/azkaban-users-tests.xml");
        CognitoUserManager userManager = new CognitoUserManager(props);
        assertEquals(expected, userManager.validateProxyUser(proxyUser, new User(user)));
    }
}
