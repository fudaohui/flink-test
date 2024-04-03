package org.fdh.day06;

import com.googlecode.aviator.AviatorEvaluator;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@Slf4j
public class AviatorBatchTest {

    private static final Random random = new Random();
    static String[] fields = {"a.a1", "b.b1", "c.c1", "d.d1", "e.e1", "f.f1", "g.g1", "h.h1", "i.i1", "j.j1"};

    public static void main(String[] args) {
        int count = 50;
        String[] expressions = new String[count];
        for (int i = 0; i < count; i++) {
            String expression = generateExpression();
            expressions[i] = expression;
        }

        int count1 = 1;
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < count1; i++) {
            for (String field : fields) {
                map.put(field, random.nextInt(100) + 1);
            }
            long timeMillis = System.currentTimeMillis();
            for (String expression : expressions) {
                long timeMillis11 = System.currentTimeMillis();
                boolean result = (Boolean) AviatorEvaluator.execute(expression, map);
                log.info("表达式: {},值为：{}，结果为：{},耗时：{}", expression,map.toString(), result,  (System.currentTimeMillis() - timeMillis11));
            }
            long timeMillis2 = System.currentTimeMillis();
            log.info("50个表达式计算完成耗时：{}", (timeMillis2-timeMillis));
        }
    }

    private static String generateExpression() {
        String[] operators = {"&&", "||"};
        String[] comparators = {">", "<", ">=", "<=", "==", "!="};
        StringBuilder expression = new StringBuilder();
        int openParenthesesCount = 0;

        for (int i = 0; i < fields.length; i++) {
            //除了第一个，后面都需要添加逻辑运算符
            if (i > 0) {
                // Append a logical operator
                expression.append(" ");
                expression.append(operators[random.nextInt(operators.length)]);
                expression.append(" ");
            }

            // Randomly decide to add an opening parenthesis
            // 每一个属性开始前都需要随机添加括号，左括号记录+1
            if (random.nextBoolean()) {
                expression.append("(");
                openParenthesesCount++;
            }

            // Append the field and its comparison
            // 添加属性和比较以及值
            expression.append(fields[i]);
            expression.append(" ");
            expression.append(comparators[random.nextInt(comparators.length)]);
            expression.append(" ");
            // Append a random integer value between 1 and 100
            expression.append(random.nextInt(100) + 1);

            // Randomly decide to close a parenthesis if there's any open
            // 随机决定要不要关闭括号
            if (openParenthesesCount > 0 && random.nextBoolean()) {
                expression.append(")");
                openParenthesesCount--;
            }
        }

        // Close any remaining open parentheses at the end of the expression
        // 前面没有关闭括号的，这里都需要补全关闭
        while (openParenthesesCount > 0) {
            expression.append(")");
            openParenthesesCount--;
        }

        return expression.toString();
    }
}
