package com.grouphigh.stream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.IOUtils;

/**
 * An implementation of GroupHigh's Stream API.
 * 
 * @author David Pinto
 */
public class Stream {

    // global constants
    private static final String ENDPOINT_STREAM = "http://services2.grouphigh.com/v4/stream/";
    private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

    /**
     * returns an iterable object of blog-post-events
     *
     * @param key account key
     * @param secret account secret
     * @return an iterable object of blog-post-events
     * @throws MalformedURLException
     * @throws IOException
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    public static Iterable<BlogPostEvent> stream(String key, String secret) throws MalformedURLException, IOException, NoSuchAlgorithmException, InvalidKeyException {
        // initialize endpoint
        final HttpURLConnection httpConnection = connect(ENDPOINT_STREAM, key, secret);
        System.out.println("RT : " + httpConnection.getReadTimeout());

        // intialize streams
        final InputStream inputStream = httpConnection.getInputStream();
        //final GZIPInputStream gzip = new GZIPInputStream(inputStream);

        // leverage jackson's json-stream api
        final JsonFactory jsonFactory = new JsonFactory();
        final JsonParser jsonParser = jsonFactory.createJsonParser(inputStream);

        // return iterable
        return new Iterable<BlogPostEvent>() {
            @Override
            public Iterator<BlogPostEvent> iterator() {
                return new Iterator<BlogPostEvent>() {
                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public BlogPostEvent next() {
                        BlogPostEvent result = null;
                        try {
                            result = new BlogPostEvent(jsonParser);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return result;
                    }

                    @Override
                    public void remove() {
                    }
                };
            }
        };
    }

    /**
     * returns a json representation of all schemas associated with the supplied
     * parameters
     *
     * @param key account key
     * @param secret account secret
     * @return a json representation of all schemas associated with the supplied
     * parameters
     */
    public static String getSchemas(String key, String secret) {
        // initialize result
        String result = null;

        // populate result
        try {
            final HttpURLConnection httpConnection = connect(ENDPOINT_STREAM + "schemas/", key, secret);
            try (final InputStream inputStream = httpConnection.getInputStream()) {
                result = IOUtils.toString(inputStream);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // return result
        return result;
    }

    /**
     * returns a json representation of a particular schema
     *
     * @param topic target topic
     * @param key account key
     * @param secret account secret
     * @return a json representation of a particular schema
     */
    public static String getSchema(String topic, String key, String secret) {
        // initialize result
        String result = null;

        // populate result
        try {
            final HttpURLConnection httpConnection = connect(ENDPOINT_STREAM + "schemas/" + URLEncoder.encode(topic, "UTF-8"), key, secret);
            result = IOUtils.toString(httpConnection.getInputStream());
        } catch (Exception e) {
            e.printStackTrace();
        }

        // return result
        return result;
    }

    /**
     * inserts/updates a given topic with the supplied schema
     *
     * @param topic target topic
     * @param schema schema to use for topic
     * @param key account key
     * @param secret account secret
     * @return status message
     */
    public static String putSchema(String topic, String schema, String key, String secret) {
        // initialize result
        String result = null;

        // populate result
        try {
            final HttpURLConnection httpConnection = connect(ENDPOINT_STREAM + "schemas/" + URLEncoder.encode(topic, "UTF-8"), key, secret);
            httpConnection.setRequestMethod("PUT");
            httpConnection.setDoOutput(true);

            try (final OutputStream outputStream = httpConnection.getOutputStream()) {
                IOUtils.write(schema, outputStream);
            }

            try (final InputStream inputStream = httpConnection.getInputStream()) {
                result = IOUtils.toString(inputStream);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // return result
        return result;
    }

    /**
     * deletes a given topic
     *
     * @param topic target topic
     * @param key account key
     * @param secret account secret
     * @return status message
     */
    public static String deleteSchema(String topic, String key, String secret) {
        // initialize result
        String result = null;

        // populate result
        try {
            final HttpURLConnection httpConnection = connect(ENDPOINT_STREAM + "schemas/" + URLEncoder.encode(topic, "UTF-8"), key, secret);
            httpConnection.setRequestMethod("DELETE");

            try (final InputStream inputStream = httpConnection.getInputStream()) {
                result = IOUtils.toString(inputStream);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // return result
        return result;
    }

    /**
     * creates a HttpURLConnection object with appropriate headers set
     * @param endpoint target endpoint
     * @param key account key
     * @param secret account secret
     * @return a populated HttpURLConnection to use
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     * @throws MalformedURLException
     * @throws IOException 
     */
    private static HttpURLConnection connect(String endpoint, String key, String secret) throws NoSuchAlgorithmException, InvalidKeyException, MalformedURLException, IOException {
        // setup endpoint
        final URL url = new URL(endpoint);
        final HttpURLConnection httpConnection = (HttpURLConnection) url.openConnection();

        // initialize crypto
        final SecretKeySpec signingKey = new SecretKeySpec(secret.getBytes(), HMAC_SHA1_ALGORITHM);
        final Mac mac = Mac.getInstance(HMAC_SHA1_ALGORITHM);
        mac.init(signingKey);

        // create timestamp and sign
        final String timestamp = Long.toString(System.currentTimeMillis());
        final String signature = Base64.encodeBase64String(mac.doFinal(timestamp.getBytes()));

        // popluate request headers
        httpConnection.setRequestProperty("Grouphigh-Key", key);
        httpConnection.setRequestProperty("Grouphigh-Timestamp", timestamp);
        httpConnection.setRequestProperty("Grouphigh-Signature", signature);

        // return connection
        return httpConnection;
    }
}
