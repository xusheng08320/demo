package com.xusheng.easy.rule;

import lombok.Data;

/**
 * @Author xusheng
 * @Date 2023/5/9 15:53
 * @Desc
 */

@Data
public class ReqContext {

    private Req req;

    private LimitRule limitRule;
}
