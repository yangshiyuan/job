package com.example.repair.controller;

import org.springframework.core.io.ClassPathResource;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.InputStream;

@RestController
public class UpdateRevenueController {


    @RequestMapping("update")
    public void update(){

        ClassPathResource classPathResource = new ClassPathResource("excleTemplate/test.xlsx");
        InputStream inputStream =classPathResource.getInputStream();

    }

}
