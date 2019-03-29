package com.xueyou.searchserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@SpringBootApplication
@ComponentScan(value="com.xueyou.searchserver.config")
public class SearchserverApplication {

    public static void main(String[] args) {
        SpringApplication.run(SearchserverApplication.class, args);
    }

}
