package org.fdh.day06;

import com.googlecode.aviator.AviatorEvaluator;

import java.util.HashMap;
import java.util.Map;

public class AviatorExample {
    public static void main(String[] args) {
        // 创建一个环境变量用来存储表达式中的变量和值
        Map<String, Object> env = new HashMap<String, Object>();
        env.put("a", 2);
        env.put("b", -1);

        // 使用 AviatorEvaluator 执行表达式
        Boolean result = (Boolean) AviatorEvaluator.execute("a > 1 && b <= 0", env);

        System.out.println("The result of the expression is: " + result);
        // 输出应该为：The result of the expression is: true
    }
}
