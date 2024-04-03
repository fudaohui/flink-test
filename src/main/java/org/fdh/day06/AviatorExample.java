package org.fdh.day06;

import com.googlecode.aviator.AviatorEvaluator;

import java.util.HashMap;
import java.util.Map;

public class AviatorExample {
    public static void main(String[] args) {
        // 创建一个环境变量用来存储表达式中的变量和值
        Map<String, Object> env = new HashMap<String, Object>();
        env.put("a.a1", 2);
        env.put("b.b1", -1);
        env.put("c.c1", 100);
        env.put("d.d1", -1);
        env.put("e.e1", -1);
        env.put("f.f1", -1);
        env.put("g.g1", -1);
        env.put("h.h1", -1);
        env.put("i.i1", -1);
        env.put("j.j1", -1);

        // 使用 AviatorEvaluator 执行表达式
        Boolean result = (Boolean) AviatorEvaluator.execute("a > 1 && b <= 0", env);

        System.out.println("The result of the expression is: " + result);
        // 输出应该为：The result of the expression is: true
    }
}
