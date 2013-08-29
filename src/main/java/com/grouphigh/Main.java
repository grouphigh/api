package com.grouphigh;

import com.grouphigh.stream.Stream;
import java.util.Arrays;
import java.util.Date;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;

/**
 * Entrypoint for API examples.
 *
 * @author David Pinto
 */
public class Main {

    private static void stream(String key, String secret, Long offset) throws Exception {
        for (JSONObject json : Stream.stream(key, secret, offset)) {
            try {
                JSONObject json_rss = json.getJSONObject("rss");
                JSONObject json_grouphigh = json.getJSONObject("grouphigh");
                JSONObject json_html = json.getJSONObject("html");

                System.out.println(json_grouphigh.get("streaming_offset") + " | " + new Date(json_rss.getLong("published")) + " | " + json_grouphigh.getString("language") + " | " + json_rss.getString("id"));
            } catch (Exception e) {
                System.out.println("...error");
                //e.printStackTrace();
                e.printStackTrace();
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
        String key = "";  // account key
        String secret = "";  // account secrect

        // create example schema
        final JSONObject schema = new JSONObject();
        schema.put("languages", new JSONArray(Arrays.asList("en", "fr", "es")));
        schema.put("minSEOMozFRMP", 2.0);
        // schema.put("blogs", new JSONArray(Arrays.asList("http://techcrunch.com/", "http://carrotsncake.com/", "http://www.androidcentral.com/", "http://androidandme.com/", "http://www.engadget.com/")));
        // add rules
        final JSONObject rule_or = new JSONObject();
        rule_or.put("operator", "OR");
        rule_or.put("keywords", new JSONArray(Arrays.asList("obama")));
        //schema.put("rules", new JSONArray(Arrays.asList(rule_or)));

        // TAKE ANY ONE OF THESE ACTIONS
        //putSchema("politics", schema.toString(), key, secret);
        //getSchema("politics", key, secret);
        //deleteSchema("politics", key, secret);
        //getSchemas(key, secret);
        //stream(key, secret, null);
    }
}