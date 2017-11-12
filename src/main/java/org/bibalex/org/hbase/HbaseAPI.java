package org.bibalex.org.hbase;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;



@SpringBootApplication
public class HbaseAPI extends SpringBootServletInitializer   {

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(HbaseAPI.class);
    }



    public static void main(String[] args) throws Exception {
        SpringApplication.run(HbaseAPI.class, args);
    }
}

//extends SpringBootServletInitializer