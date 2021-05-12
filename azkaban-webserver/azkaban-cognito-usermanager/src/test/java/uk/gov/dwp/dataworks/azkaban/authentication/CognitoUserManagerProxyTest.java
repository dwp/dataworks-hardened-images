package uk.gov.dwp.dataworks.azkaban.authentication;

import azkaban.user.User;
import azkaban.user.UserManagerException;
import azkaban.utils.Props;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.AuthenticationResultType;
import software.amazon.awssdk.services.cognitoidentityprovider.model.InitiateAuthRequest;
import software.amazon.awssdk.services.cognitoidentityprovider.model.InitiateAuthResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.model.NotAuthorizedException;

import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.*;

import static org.mockito.Mockito.when;

public class CognitoUserManagerProxyTest {

    @Mock
    private CognitoIdentityProviderClient mockCognitoIdentityProvider;

    @Mock
    private InitiateAuthResponse mockResponse;

    @Mock
    private AuthenticationResultType mockResultType;

    @Mock
    private Props mockProps;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        when(mockProps.getString("aws.region", "EU-WEST-2")).thenReturn("EU-WEST-2");
        when(mockProps.getString("cognito.clientId")).thenReturn("testClientId");
        when(mockProps.getString("cognito.clientSecret")).thenReturn("clientSecret");
    }

    @Test
    public void shouldAttemptLoginSuccessfull() throws UserManagerException {

        when(mockResponse.authenticationResult()).thenReturn(mockResultType);
        when(mockCognitoIdentityProvider.initiateAuth((Consumer<InitiateAuthRequest.Builder>) Mockito.any())).thenReturn(mockResponse);
        CognitoUserManagerProxy proxy = new CognitoUserManagerProxy(mockProps);
        proxy.setIdentityProvider(mockCognitoIdentityProvider);

        User result = proxy.getUser("user", "good!");

        assertThat(result).isNotNull();
        assertThat(result.getUserId()).isEqualTo("user");

    }

    @Test()
    public void shouldAttemptLoginFailure() throws UserManagerException {

        when(mockResponse.authenticationResult()).thenReturn(null);
        when(mockCognitoIdentityProvider.initiateAuth((Consumer<InitiateAuthRequest.Builder>) Mockito.any())).thenThrow(NotAuthorizedException.class);

        CognitoUserManagerProxy proxy = new CognitoUserManagerProxy(mockProps);
        assertThatExceptionOfType(UserManagerException.class).isThrownBy( () -> {
            User result = proxy.getUser("user", "bad");
        }).withMessageStartingWith("User pool client testClientId does not exist.");
    }
}
