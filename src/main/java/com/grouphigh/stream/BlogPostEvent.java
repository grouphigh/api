package com.grouphigh.stream;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Encapsulates a single blog-post event.
 *
 * @author David Pinto
 */
public class BlogPostEvent {

    // header fields
    private String id_client;
    private String id_topic;
    private Date timestamp;
    // post fields
    private String id;
    private String title;
    private Date published;
    private String author;
    private String language;
    private List<String> blogs = new ArrayList<>();
    private List<String> feeds = new ArrayList<>();
    private List<String> topics = new ArrayList<>();
    private List<String> content = new ArrayList<>();
    private String description;

    /* *************************************************************************
     * CONSTRUCTORS
     * ************************************************************************/
    /**
     * create an object using a JsonParser object
     *
     * @param jp
     * @throws IOException
     */
    public BlogPostEvent(JsonParser jp) throws IOException {
        // check for start of object
        if (jp.nextToken() != JsonToken.START_OBJECT) {
            throw new IOException("Expected data to start with an Object");
        }
        boolean inPost = false;
        while ((jp.nextToken() != JsonToken.END_OBJECT) || inPost) {
            JsonToken currentToken = jp.getCurrentToken();

            // get current field name and value token
            final String fieldName = jp.getCurrentName();
            if (!fieldName.equals("post")) {
                currentToken = jp.nextToken();
            }

            if (fieldName != null) {
                switch (fieldName) {
                    case "id_client":
                        id_client = jp.getText();
                        break;
                    case "id_topic":
                        id_topic = jp.getText();
                        break;
                    case "timestamp":
                        timestamp = new Date(jp.getLongValue());
                        break;
                    case "post":
                        inPost = jp.getCurrentToken() != JsonToken.END_OBJECT;
                        break;
                    case "id":
                        id = jp.getText();
                        break;
                    case "title":
                        title = jp.getText();
                        break;
                    case "language":
                        language = jp.getText();
                        break;
                    case "author":
                        author = jp.getText();
                        break;
                    case "description":
                        description = jp.getText();
                        break;
                    case "published":
                        published = new Date(jp.getLongValue());
                        break;
                    case "blogs":
                        while ((currentToken = jp.nextToken()) != JsonToken.END_ARRAY) {
                            blogs.add(jp.getText());
                        }
                        break;
                    case "feeds":
                        while ((currentToken = jp.nextToken()) != JsonToken.END_ARRAY) {
                            feeds.add(jp.getText());
                        }
                        break;
                    case "topics":
                        while ((currentToken = jp.nextToken()) != JsonToken.END_ARRAY) {
                            topics.add(jp.getText());
                        }
                        break;
                    case "contents":
                        while ((currentToken = jp.nextToken()) != JsonToken.END_ARRAY) {
                            content.add(jp.getText());
                        }
                        break;
                    default:
                        System.out.println(fieldName + " : " + jp.getText());
                }
            }
        }

    }

    /* *************************************************************************
     * GETTER & SETTER METHODS
     * ************************************************************************/
    public String getId_client() {
        return id_client;
    }

    public void setId_client(String id_client) {
        this.id_client = id_client;
    }

    public String getId_topic() {
        return id_topic;
    }

    public void setId_topic(String id_topic) {
        this.id_topic = id_topic;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Date getPublished() {
        return published;
    }

    public void setPublished(Date published) {
        this.published = published;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public List<String> getBlogs() {
        return blogs;
    }

    public void setBlogs(List<String> blogs) {
        this.blogs = blogs;
    }

    public List<String> getFeeds() {
        return feeds;
    }

    public void setFeeds(List<String> feeds) {
        this.feeds = feeds;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public List<String> getContent() {
        return content;
    }

    public void setContent(List<String> content) {
        this.content = content;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
