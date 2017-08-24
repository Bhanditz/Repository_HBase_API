package org.bibalex.org.hbase;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


@SpringBootApplication
public class HbaseAPI {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(HbaseAPI.class, args);
    }
}