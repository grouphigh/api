package com.grouphigh;

import com.grouphigh.stream.Stream;
import java.util.Arrays;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

/**
 * Entrypoint for API examples.
 *
 * @author David Pinto
 */
public class Main {

    private static void stream(String key, String secret) throws Exception {
        for (JSONObject json : Stream.stream(key, secret)) {
            System.out.println("topic : " + json.getString("id_topic"));
            System.out.println("\ttitle : " + json.getJSONObject("post").getString("title"));
            System.out.println("\tlanguage : " + json.getJSONObject("post").getString("language"));
            final JSONArray array = json.getJSONObject("post").getJSONArray("blogs");
            for (int i = 0; i < array.length(); i++) {
                final JSONObject blog = array.getJSONObject(i);
                System.out.println("\t\t" + blog.getString("id") + " ( " + blog.optDouble("seomoz_fmrp") + " )");
            }
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
        
        // create example schema
        final JSONObject schema = new JSONObject();
        schema.put("languages", new JSONArray(Arrays.asList("en", "es", "fr")));
        schema.put("minSEOMozFRMP", 2.0);
        schema.put("blogs", new JSONArray(Arrays.asList("http://techcrunch.com/", "http://carrotsncake.com/", "http://www.androidcentral.com/", "http://androidandme.com/", "http://www.engadget.com/")));
        // add rules
        final JSONObject rule_or = new JSONObject();
        rule_or.put("operator", "OR");
        rule_or.put("keywords", new JSONArray(Arrays.asList("apple", "food")));
        schema.put("rules", new JSONArray(Arrays.asList(rule_or)));

        //putSchema("example", schema.toString(), key, secret);
        //getSchema("example", key, secret);
        //deleteSchema("example", key, secret);
        //getSchemas(key, secret);
        //stream(key, secret);
    }
}