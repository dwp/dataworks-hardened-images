# Livy-proxy

Squid proxy that sits in front of a Apache Livy instance to perform JWT based authentication and ensure that the
`proxy_user` Livy request body param is set to the correct username if present. 

The Docker image requires 2 environment variables: 
* `LIVY_URL` - the URL where the sucessfully authenticated requests are proxied to
* `KEYSTORE_DATA` - The JWK keystore data, encoded in base 64. 
