package cecs429.text;

import java.util.ArrayList;
import java.util.List;
import org.tartarus.snowball.ext.PorterStemmer;

/**
 * A BasicTokenProcessor creates terms from tokens by removing all
 * non-alphanumeric characters from the token, and converting it to all
 * lowercase.
 */
public class AdvancedTokenProcessor implements TokenProcessor {

    private static PorterStemmer stemmer = new PorterStemmer();

    @Override
    public List<String> processToken(String token) {
        List<String> result = new ArrayList<String>();
        String[] substr = null;
        token = token.trim().toLowerCase();
        int start, end, strStart = 0, strEnd = 0, flag1 = 0, flag2 = 0;
        for (start = 0; start < token.length(); start++) {
            if (!Character.isLetterOrDigit(token.charAt(start))) {
                continue;
            } else {
                strStart = start;
                flag1++;
                break;
            }
        }
        for (end = token.length() - 1; end >= strStart; end--) {
            if (!Character.isLetterOrDigit(token.charAt(end))) {
                continue;
            } else {
                strEnd = end;
                flag2++;
                break;
            }
        }
        if (flag1 == 0 || flag2 == 0) {
            token = "";
        } else {
            token = token.substring(strStart, strEnd + 1);
        }
        token = token.replace("\"", "");
        token = token.replace("'", "");
        if (token.contains("-")) {
            substr = token.split("-");
            int count = 0;
            for (count = 0; count < substr.length; count++) {
                if (!substr[count].isEmpty()) {
                    List<String> temp = processToken(substr[count]);
                    result.add(stem(temp.get(0)));

                }
            }
            String flag = token.replace("-", "");
            result.add(stem(flag));

        } else {
            result.add(stem(token));

        }

        return result;
    }

    public String stem(String s) {

        stemmer.setCurrent(s); //set string you need to stem
        stemmer.stem();  //stem the word
        return stemmer.getCurrent();//get the stemmed word
    }
}
