package uk.gov.dwp.dataworks.azkaban.authentication;

import azkaban.user.Role;
import azkaban.user.User;
import azkaban.user.UserManager;
import azkaban.user.UserManagerException;
import azkaban.utils.Props;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.AuthFlowType;
import software.amazon.awssdk.services.cognitoidentityprovider.model.InitiateAuthResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.model.NotAuthorizedException;
import software.amazon.awssdk.services.cognitoidentityprovider.model.ResourceNotFoundException;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

public class CognitoUserManagerProxy implements UserManager {
    private final Props props;
    private CognitoIdentityProviderClient identityProvider;

    public CognitoUserManagerProxy(final Props props) {
        this.props = props;
        this.identityProvider = CognitoIdentityProviderClient.builder()
                .region(Region.of(props.getString("aws.region", "EU-WEST-2")))
                .build();
    }

    @Override
    public User getUser(String username, String password) throws UserManagerException {

        Map<String, String> authParameters = new HashMap<>();
        authParameters.put("USERNAME", username);
        authParameters.put("PASSWORD", password);
        authParameters.put("SECRET_HASH",
                calculateSecretHash(props.getString("cognito.clientId"),
                                    props.getString("cognito.clientSecret"),
                                    username));

        final InitiateAuthResponse response;
        try {
            response = identityProvider.initiateAuth(builder -> {
                builder.authFlow(AuthFlowType.USER_PASSWORD_AUTH)
                        .authParameters(authParameters)
                        .clientId(props.getString("cognito.clientId"));
            });
        } catch (NotAuthorizedException ex) {
            throw new UserManagerException("Incorrect username or password");
        } catch (ResourceNotFoundException ex) {
            throw new UserManagerException(ex.getMessage());
        }

        if (response.authenticationResult() != null) {
            // Success
            User u = new User(username);
            User.UserPermissions permissions = new User.UserPermissions() {
                @Override
                public boolean hasPermission(String permission) {
                    return true;
                }

                @Override
                public void addPermission(String permission) {

                }
            };

            u.setPermissions(permissions);

            return u;
        }

        return null;
    }

    public static String calculateSecretHash(String userPoolClientId, String userPoolClientSecret, String userName) {
        final String HMAC_SHA256_ALGORITHM = "HmacSHA256";

        SecretKeySpec signingKey = new SecretKeySpec(
                userPoolClientSecret.getBytes(StandardCharsets.UTF_8),
                HMAC_SHA256_ALGORITHM);
        try {
            Mac mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
            mac.init(signingKey);
            mac.update(userName.getBytes(StandardCharsets.UTF_8));
            byte[] rawHmac = mac.doFinal(userPoolClientId.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(rawHmac);
        } catch (Exception e) {
            throw new RuntimeException("Error while calculating ");
        }
    }

    @Override
    public boolean validateUser(String username) {
        return true;
    }

    @Override
    public boolean validateGroup(String group) {
        return true;
    }

    @Override
    public Role getRole(String roleName) {
        return null;
    }

    @Override
    public boolean validateProxyUser(String proxyUser, User realUser) {
        return false;
    }

    /* package-private */ void setIdentityProvider(CognitoIdentityProviderClient identityProvider) {
        this.identityProvider = identityProvider;
    }
}
