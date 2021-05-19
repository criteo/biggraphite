# Cassandra SSL certificate creation

If anybody needs to test cassandra with ssl certificates, this is we created & used our certificates for testing.

This documentation is just for testing. You should roll out your own certificates using official documentation.

## Documentation

* https://docs.datastax.com/en/cassandra-oss/3.0/cassandra/configuration/secureSSLCertWithCA.html
* https://docs.datastax.com/en/security/6.0/security/secSetUpSSLCert.html
* https://docs.datastax.com/en/security/6.0/security/Auth/secCqlshSsl.html

## Certificate creation

### In `config/rootca.conf`

```
[ req ]
distinguished_name = req_distinguished_name
prompt             = no
output_password    = cassandra
default_bits       = 2048

[ req_distinguished_name ]
C  = FR
O  = Criteo
OU = Biggraphite
CN = rootCa
```

### Create CA

```sh
openssl req -config config/rootca.conf -new -x509 -nodes -subj /CN=rootCa/OU=Biggraphite/O=Criteo/C=FR/ -keyout root.key -out root.crt -days 3650
openssl x509 -in root.crt -text -noout
```

### Create each server node certificate in keystore

```
keytool -genkeypair -keyalg RSA -alias 127.0.0.1 -deststoretype pkcs12 -keystore 127.0.0.1.jks -storepass cassandra -keypass cassandra \
  -validity 3650 -keysize 2048 -dname "CN=127.0.0.1, OU=Biggraphite, O=Criteo, C=FR" -ext "SAN=IP:127.0.0.1"
keytool -v -list -keystore 127.0.0.1.jks -storepass cassandra
```

### In `config/san_config_file.conf`:

```
subjectAltName=DNS:localhost,IP:127.0.0.1
```

### Export CSR for each node, to create signed certificate

```
keytool -certreq -keystore 127.0.0.1.jks -alias 127.0.0.1 -file 127.0.0.1.csr -deststoretype pkcs12 -keypass cassandra -storepass cassandra \
  -dname "CN=127.0.0.1, OU=Biggraphite, O=Criteo, C=FR" -ext "SAN=IP:127.0.0.1"

openssl x509 -req -CA root.crt -CAkey root.key -in 127.0.0.1.csr -out 127.0.0.1.crt -days 36500 -CAcreateserial -passin pass:cassandra -extfile config/san_config_file.conf
openssl verify -CAfile root.crt 127.0.0.1.crt
```

### Import root CA in keystore

```
keytool -importcert -keystore 127.0.0.1.jks -alias root -file root.crt -noprompt -keypass cassandra -storepass cassandra
keytool -list -keystore 127.0.0.1.jks -storepass cassandra
```

### Import signed certificate

```
keytool -importcert -keystore 127.0.0.1.jks -alias 127.0.0.1 -file 127.0.0.1.crt -noprompt -keypass cassandra -storepass cassandra
keytool -list -keystore 127.0.0.1.jks -storepass cassandra
```

### Create a truststore

```
keytool -importcert -keystore truststore.jks -alias root -file root.crt -noprompt -keypass cassandra -storepass cassandra
keytool -list -keystore truststore.jks -storepass cassandra
```

### In cassandra configuration:

```
client_encryption_options:
  enabled: true
  optional: false
  keystore: /path/to/127.0.0.1.jks
  keystore_password: cassandra
  require_client_auth: true
  truststore: /path/to/truststore.jks
  truststore_password: cassandra
```


## cqlsh client certificate creation

### In `config/client.conf`

```
[ req ]
distinguished_name = CA_DN
prompt             = no
output_password    = cassandra
default_bits       = 2048

[ CA_DN ]
C  = FR
O  = Criteo
OU = Biggraphite
CN = CA_CN
```

### Create client certificate:

```
openssl req -newkey rsa:2048 -nodes -keyout client_key.key -out signing_request.csr -config config/client.conf
openssl x509 -req -CA root.crt -CAkey root.key -in signing_request.csr -out client_cert.crt_signed -days 3650 -CAcreateserial -passin pass:cassandra
```

### Complete cqlsh configuration:

```
[ssl]
certfile = /path/to/root.crt
validate = false
userkey = /path/to/client_key.key
usercert = /path/to/client_cert.crt_signed
version = TLSv1_2
```


## Test with bgutil:

```
bgutil \
    --cassandra_username cassandra \
    --cassandra_password cassandra \
    --cassandra_ssl_enable \
    --cassandra_ssl_verify_locations /path/to/root.crt \
    --cassandra_ssl_cert_file /path/to/client_cert.crt_signed \
    --cassandra_ssl_key_file /path/to/client_key.key \
    --cassandra_ssl_check_hostname 127.0.0.1 \
    list
```

## Test with carbon:

```
BG_CASSANDRA_USERNAME = cassandra
BG_CASSANDRA_PASSWORD = cassandra

BG_CASSANDRA_SSL_ENABLE = True
BG_CASSANDRA_SSL_VERIFY_LOCATIONS = /path/to/root.crt
BG_CASSANDRA_SSL_CERT_FILE = /path/to/client_cert.crt_signed
BG_CASSANDRA_SSL_KEY_FILE = /path/to/client_key.key
BG_CASSANDRA_SSL_CHECK_HOSTNAME = 127.0.0.1

BG_CASSANDRA_SSL_ENABLE_METADATA = True
BG_CASSANDRA_SSL_VERIFY_LOCATIONS_METADATA = /path/to/root.crt
BG_CASSANDRA_SSL_CERT_FILE_METADATA = /path/to/client_cert.crt_signed
BG_CASSANDRA_SSL_KEY_FILE_METADATA = /path/to/client_key.key
BG_CASSANDRA_SSL_CHECK_HOSTNAME_METADATA = 127.0.0.1
```

## Some errors

### With ssl required but disabled on client side:

```
cassandra.cluster.NoHostAvailable: ('Unable to connect to any servers', {'127.0.0.1:9042': ConnectionShutdown('Connection to 127.0.0.1:9042 was closed')})
```

### With an invalid user/password:

```
cassandra.cluster.NoHostAvailable: ('Unable to connect to any servers', {'127.0.0.1:9042': AuthenticationFailed('Failed to authenticate to 127.0.0.1:9042: Error from server: code=0100 [Bad credentials] message="Provided username cassandra and/or password are incorrect"')})
```

### With no CA root certificate:

```
cassandra.cluster.NoHostAvailable: ('Unable to connect to any servers', {'127.0.0.1:9042': PermissionError(1, "Tried connecting to [('127.0.0.1', 9042)]. Last error: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self signed certificate in certificate chain (_ssl.c:1123)")})
```

### With no client certificate (mandatory on server):

```
cassandra.cluster.NoHostAvailable: ('Unable to connect to any servers', {'127.0.0.1:9042': PermissionError(1, "Tried connecting to [('127.0.0.1', 9042)]. Last error: [SSL: SSLV3_ALERT_BAD_CERTIFICATE] sslv3 alert bad certificate (_ssl.c:1123)")})
```

### With invalid check hostname

```
cassandra.cluster.NoHostAvailable: ('Unable to connect to any servers', {'127.0.0.1:9042': PermissionError(1, "Tried connecting to [('127.0.0.1', 9042)]. Last error: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: IP address mismatch, certificate is not valid for '127.0.0.2'. (_ssl.c:1123)")})
```