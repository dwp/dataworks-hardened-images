package uk.gov.dwp.dataworks.azkaban.authentication;

import azkaban.user.*;
import azkaban.utils.Props;

import org.apache.http.HttpHost;
import org.apache.http.impl.conn.DefaultProxyRoutePlanner;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClientBuilder;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Logger;

public class CognitoUserManagerProxy implements UserManager {
    private static final Logger LOGGER = Logger.getLogger(CognitoUserManagerProxy.class.getName());

    private final Props props;
    private CognitoIdentityProviderClient identityProvider;
    private AuthenticationHelper authHelper;

    public CognitoUserManagerProxy(final Props props) {
        this.props = props;

        CognitoIdentityProviderClientBuilder builder = CognitoIdentityProviderClient.builder();
        builder = builder.region(Region.of(props.getString("aws.region", "eu-west-2")));

        if(props.containsKey("http.proxyHost") && props.containsKey("http.proxyPort")) {
            LOGGER.info("Using azkaban properties for proxy.");
            DefaultProxyRoutePlanner planner = new DefaultProxyRoutePlanner(new HttpHost(props.getString("http.proxyHost"), props.getInt("http.proxyPort")));
            SdkHttpClient httpClient = ApacheHttpClient.builder()
                    .httpRoutePlanner(planner)
                    .build();
            builder = builder.httpClient(httpClient);
        } else {
            LOGGER.info("Using system properties for proxy.");
            SdkHttpClient httpClient = ApacheHttpClient.builder().proxyConfiguration(
                    ProxyConfiguration.builder().useSystemPropertyValues(true).build()
            ).build();
            builder = builder.httpClient(httpClient);
        }

        this.identityProvider = builder.build();
        this.authHelper = new AuthenticationHelper(props.getString("cognito.userPoolName"));
    }

    @Override
    public User getUser(String username, String password) throws UserManagerException {

        LOGGER.entering("CognitoUserManagerProxy", "getUser", new Object[]{ username, password });

        String secretHash = AuthenticationHelper.calculateSecretHash(
                props.getString("cognito.clientId"),
                props.getString("cognito.clientSecret"),
                username);

        Map<String, String> authParameters = new HashMap<>();
        authParameters.put("USERNAME", username);
        authParameters.put("SRP_A", authHelper.getA().toString(16));
        authParameters.put("SECRET_HASH", secretHash);

        final InitiateAuthResponse response;
        AuthenticationResultType result = null;
        ChallengeNameType lastChallenge = null;
        try {
            LOGGER.info("Authenticating user " + username);
            response = identityProvider.initiateAuth(builder -> {
                builder.authFlow(AuthFlowType.USER_SRP_AUTH)
                        .authParameters(authParameters)
                        .clientId(props.getString("cognito.clientId"));
            });

            result = response.authenticationResult();
            lastChallenge = response.challengeName();

            if (ChallengeNameType.PASSWORD_VERIFIER.equals(lastChallenge)) {
                String userIdForSrp = response.challengeParameters().get("USER_ID_FOR_SRP");
                String userNameForSrp = response.challengeParameters().get("USERNAME");

                BigInteger b = new BigInteger(response.challengeParameters().get("SRP_B"), 16);
                BigInteger salt = new BigInteger(response.challengeParameters().get("SALT"), 16);

                SimpleDateFormat formatTimestamp = new SimpleDateFormat("EEE MMM d HH:mm:ss z yyyy", Locale.US);
                formatTimestamp.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));

                byte[] key = authHelper.getPasswordAuthenticationKey(userIdForSrp, password, b, salt);

                Date timestamp = new Date();
                byte[] hmac = null;
                try {
                    Mac mac = Mac.getInstance("HmacSHA256");
                    SecretKeySpec keySpec = new SecretKeySpec(key, "HmacSHA256");
                    mac.init(keySpec);
                    mac.update(props.getString("cognito.userPoolName").split("_", 2)[1].getBytes(StandardCharsets.UTF_8));
                    mac.update(userIdForSrp.getBytes(StandardCharsets.UTF_8));
                    byte[] secretBlock = Base64.getDecoder().decode(response.challengeParameters().get("SECRET_BLOCK"));
                    mac.update(secretBlock);
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE MMM d HH:mm:ss z yyyy", Locale.US);
                    simpleDateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"));
                    String dateString = simpleDateFormat.format(timestamp);
                    byte[] dateBytes = dateString.getBytes(StandardCharsets.UTF_8);
                    hmac = mac.doFinal(dateBytes);
                } catch (Exception e) {
                    System.out.println(e);
                }

                Map<String, String> challengeResponses = new HashMap<>();
                challengeResponses.put("PASSWORD_CLAIM_SECRET_BLOCK", response.challengeParameters().get("SECRET_BLOCK"));
                challengeResponses.put("PASSWORD_CLAIM_SIGNATURE", Base64.getEncoder().encodeToString(hmac));
                challengeResponses.put("USERNAME", userNameForSrp);
                challengeResponses.put("TIMESTAMP", formatTimestamp.format(timestamp));
                challengeResponses.put("SECRET_HASH", secretHash);

                RespondToAuthChallengeRequest challengeResponse = RespondToAuthChallengeRequest.builder()
                        .challengeName(lastChallenge)
                        .clientId(props.getString("cognito.clientId"))
                        .session(response.session())
                        .challengeResponses(challengeResponses)
                        .build();

                RespondToAuthChallengeResponse challengeResult = identityProvider.respondToAuthChallenge(challengeResponse);

                result = challengeResult.authenticationResult();
                lastChallenge = challengeResult.challengeName();
            }
        }
        catch (NotAuthorizedException ex) {
            LOGGER.warning(username + " is not authorized. " + ex.getMessage());
            throw new UserManagerException(ex.getMessage());
        }
        catch (UserNotFoundException ex) {
            LOGGER.warning( "Login for user " + username + " who does not exist.");
            throw new UserManagerException(ex.getMessage());
        }
        catch (Exception ex) {
            LOGGER.severe(ex.getMessage());
            throw new UserManagerException(ex.getMessage());
        }

        if (result != null) {
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

            LOGGER.info("Returning user object for " + username);
            u.addRole("ADMIN");
            return u;
        }

        LOGGER.warning("Could not authenticate user, last challenge was: " + lastChallenge);
        return null;
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
        LOGGER.entering("CognitoUserManagerProxy", "getRole", new Object[]{ roleName });
        switch(roleName) {
            case "ADMIN":
                return new Role(roleName, new Permission(Permission.Type.ADMIN));
            default:
                return new Role(roleName, new Permission(Permission.Type.READ));
        }
    }

    @Override
    public boolean validateProxyUser(String proxyUser, User realUser) {
        return false;
    }

    /* package-private */ void setIdentityProvider(CognitoIdentityProviderClient identityProvider) {
        this.identityProvider = identityProvider;
    }

}
