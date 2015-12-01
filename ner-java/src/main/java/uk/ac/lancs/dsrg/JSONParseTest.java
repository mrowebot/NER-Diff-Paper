package uk.ac.lancs.dsrg;

import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by rowem on 18/11/15.
 */
public class JSONParseTest {

    public static void main(String[] args) {

        String s = "{\"link_id\": \"t3_5zep2\", \"author\": \"M0b1u5\", \"distinguished\": null, \"score_hidden\": false, \"controversiality\": 0, \"subreddit\": \"politics\", \"retrieved_on\": 1427424835, \"created_utc\": \"1193875193\", \"id\": \"c02cheu\", \"subreddit_id\": \"t5_2cneq\", \"author_flair_text\": null, \"downs\": 0, \"edited\": false, \"gilded\": 0, \"name\": \"t1_c02cheu\", \"body\": \"US: We are fucking idiots.\", \"archived\": true, \"score\": 1, \"entity_texts\": [\"US\", \"Peter Andre\"], \"author_flair_css_class\": null, \"parent_id\": \"t3_5zep2\", \"ups\": 1}\n";

        HashSet<String> entities = new HashSet<String>();
        Pattern pattern = Pattern.compile("\\[\".+\"\\]");
        Matcher m = pattern.matcher(s);
        if (m.find()) {
            String matchedList = m.group();
            if (matchedList.length() > 2) {
                String[] entityToks = matchedList.replace("[","").replace("]","").split("\", \"");
                for (String entity : entityToks) {
                    entities.add(entity.replace("\"", "").trim());
                }
            }
        }

        for (String entity : entities) {
            System.out.println(entity);
        }
    }
}
