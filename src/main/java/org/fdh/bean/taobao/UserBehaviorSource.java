package org.fdh.bean.taobao;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class UserBehaviorSource implements SourceFunction<UserBehavior> {

    private boolean isRunning = true;
    private InputStream streamSource;
    private String path;
    private boolean isFirstLine = true;

    public UserBehaviorSource(String path) {
        this.path = path;
    }


    @Override
    public void run(SourceContext<UserBehavior> ctx) throws Exception {
        streamSource = this.getClass().getClassLoader().getResourceAsStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(streamSource));
        String lineStr;
        long lastTs = 0;
        while (isRunning && !StringUtils.isEmpty(lineStr = br.readLine())) {
            String[] split = lineStr.split(",");
            long eventTs = Long.parseLong(split[4]);
            if (isFirstLine) {
                isFirstLine = false;
                lastTs = eventTs;
            }
            UserBehavior userBehavior = UserBehavior.of(Long.parseLong(split[0]), Long.parseLong(split[1]),
                    Integer.parseInt(split[2]), split[3], eventTs);
            ctx.collect(userBehavior);
            if ((eventTs - lastTs) > 0) {
                Thread.sleep(eventTs - lastTs);
            }
            lastTs = eventTs;
        }
    }

    @Override
    public void cancel() {

        try {
            streamSource.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        isRunning = false;
    }
}
