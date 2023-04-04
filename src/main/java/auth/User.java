package auth;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import com.google.common.hash.Hashing;
import support.GlobalData;

public class User {
    private BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
    public static String dbPath = "./DatabaseSystem/";

    public void registration() throws IOException {
        File file = new File(dbPath + "User_Profile.txt");
        BufferedWriter writer = new BufferedWriter(new FileWriter(file, true));
        System.out.println("Please enter all the information requested below. Press enter after entering each value");
        System.out.print("Enter your user id: ");
        String userId = reader.readLine();
        String hashedId = Hashing.sha256()
                .hashString(userId, StandardCharsets.UTF_8)
                .toString();
        System.out.print("Enter your password: ");
        String password = reader.readLine();
        String hashedPassword = Hashing.sha256()
                .hashString(password, StandardCharsets.UTF_8)
                .toString();
        System.out.print("Enter first security question: ");
        String question1 = reader.readLine();
        System.out.print("Enter first security question answer: ");
        String answer1 = reader.readLine();
        System.out.print("Enter second security question: ");
        String question2 = reader.readLine();
        System.out.print("Enter second security question answer: ");
        String answer2 = reader.readLine();
        String formatted = hashedId + "<xx>" + hashedPassword + "<xx>" + question1 + "<xx>" + answer1 + "<xx>" + question2 + "<xx>" + answer2;
        writer.write(formatted);
        writer.close();
    }

    public boolean authenticate() throws IOException {
        File file = new File(dbPath + "User_Profile.txt");
        System.out.println("Please enter all the information requested below. Press enter after entering each value");
        System.out.print("Enter your user id: ");
        String userId = reader.readLine();
        String hashedId = Hashing.sha256()
                .hashString(userId, StandardCharsets.UTF_8)
                .toString();
        System.out.print("Enter your password: ");
        String password = reader.readLine();
        String hashedPassword = Hashing.sha256()
                .hashString(password, StandardCharsets.UTF_8)
                .toString();
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while ((line = br.readLine()) != null) {
            String[] values = line.split("<xx>");
            if(values[0].equals(hashedId)) {
                if(values[1].equals(hashedPassword)) {
                    int selectedNumber = generateRandom();
                    System.out.println("For security question, " + values[selectedNumber]);
                    System.out.print("Enter answer: ");
                    String answer = reader.readLine();
                    if (answer.equals(values[selectedNumber + 1])) {
                        GlobalData.userId=userId;
                        return true;
                    }
                }
            }

        }
        return false;
    }

    private int generateRandom() {
        int first = 2;
        int second = 4;
        int randomNumber = new Random().nextBoolean() ? first : second;
        return randomNumber;
    }
}
