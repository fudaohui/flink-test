package org.fdh.day05;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Test {
    public static void main(String[] args) {
        String time = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        time = time.substring(0, 19).replace(":","_");
        System.out.println(time);
    }
}
