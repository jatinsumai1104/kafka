package com.kafka.facebook;

import com.restfb.Connection;
import com.restfb.DefaultFacebookClient;
import com.restfb.Version;
import com.restfb.types.Post;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

public class FacebookClient {

    String appId = "";
    String appSecret = "";
    String accessToken = "";



    public static void main(String[] args) {
        new FacebookClient().run();
    }

    public void run(){

        Logger logger = LoggerFactory.getLogger(FacebookClient.class);

        DefaultFacebookClient facebook = this.createFacebook();

        Connection<Post> myFeed = facebook.fetchConnection("me/feed", Post.class);

// Get the iterator
        Iterator<List<Post>> it = myFeed.iterator();

        while(it.hasNext()) {
            List<Post> myFeedPage = it.next();

            // This is the same functionality as the example above
            for (Post post : myFeedPage) {
                logger.info("Posts: " + post);
            }
        }
    }

    public DefaultFacebookClient createFacebook(){
        DefaultFacebookClient facebook = new DefaultFacebookClient(accessToken, Version.VERSION_5_0);
        return facebook;
    }
}
