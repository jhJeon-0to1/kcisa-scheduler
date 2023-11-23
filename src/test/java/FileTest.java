import java.io.File;
import java.util.Objects;

public class FileTest {
    public static void main(String[] args) {
        try {
            File file = new File("src/main/resources/data/pet");
            File[] tempFile = file.listFiles();
            for (File value : Objects.requireNonNull(tempFile)) {
                if (!value.getName().contains(".zip")) {
                    value.delete();
                }
            }


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
