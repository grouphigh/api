package com.grouphigh;

import com.fasterxml.jackson.core.JsonParser;
import com.grouphigh.stream.BlogPostEvent;
import com.grouphigh.stream.Stream;
import java.util.concurrent.TimeUnit;

/**
 * Entrypoint for API examples.
 *
 * @author David Pinto
 */
public class Main {

    private static void stream(String key, String secret) throws Exception {
        for (BlogPostEvent blogPostEvent : Stream.stream(key, secret)) {
            System.out.println(blogPostEvent.getId_topic() + " = " + blogPostEvent.getTitle());
            System.out.println("\t" + blogPostEvent.getPublished() + " vs " + blogPostEvent.getTimestamp() + " = " + TimeUnit.MILLISECONDS.toMinutes(blogPostEvent.getTimestamp().getTime() - blogPostEvent.getPublished().getTime()));
            System.out.println("\t" + blogPostEvent.getBlogs());
            System.out.println("\t" + blogPostEvent.getLanguage());
        }
    }

    private static void getSchemas(String key, String secret) throws Exception {
        System.out.println(Stream.getSchemas(key, secret));
    }

    private static void getSchema(String topic, String key, String secret) throws Exception {
        System.out.println(Stream.getSchema(topic, key, secret));
    }

    private static void putSchema(String topic, String schema, String key, String secret) throws Exception {
        System.out.println(Stream.putSchema(topic, schema, key, secret));
    }

    private static void deleteSchema(String topic, String key, String secret) throws Exception {
        System.out.println(Stream.deleteSchema(topic, key, secret));
    }

    public static void main(String[] args) throws Exception {
        final String key = "";  // account key
        final String secret = "";  // account secrect

        // the choice of keywords is simply to generate a good bit on content
        final String schema_example = "{\"rules\":[{\"operator\":\"OR\", \"keywords\":[\"political\", \"politics\", \"obama\"]}]}";
        //putSchema("google", example, key, secret);
        //stream(key, secret);
        //deleteSchema("obama", key, secret);
        //getSchemas(key, secret);
    }
}