package org.fdh.bean.taobao;

import lombok.Data;

@Data
public class UserBehavior {
    public long userId;
    public long itemId;
    public int categoryId;
    public String behavior;
    public long timestamp;


    public UserBehavior(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }



    public static UserBehavior of(long userId, long itemId, int categoryId, String behavior, long timestamp) {
        return new UserBehavior(userId, itemId, categoryId, behavior, timestamp);
    }


}
