package scheduler.test;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@EntityScan(basePackages = {"scheduler.test.model"})
public class Application {
    public static void main(String[] args) {
        SpringApplication app =
                new SpringApplication(Application.class);

        app.run(args);
    }
}
