package com.xusheng.easy.rule;

import org.jeasy.rules.api.Condition;
import org.jeasy.rules.api.Facts;
import org.jeasy.rules.spel.SpELCondition;
import org.junit.Test;

/**
 * @Author xusheng
 * @Date 2023/4/25 14:19
 * @Desc
 */
public class EasyRuleTest {

    @Test
    public void test1() {
        ReqContext context = new ReqContext();
        Req req = new Req();
        req.setBosId(456L);
        req.setWid(1234L);
        context.setReq(req);
        LimitRule limitRule = new LimitRule();
        limitRule.setQps(1000L);
        context.setLimitRule(limitRule);
        Facts facts = new Facts();
        facts.put("context", context);

        Condition condition = new SpELCondition("#{(['context'].req.wid == 123 && ['context'].req.bosId == 456) || ['context'].limitRule.qps <= 100 } ");
        boolean evaluate = condition.evaluate(facts);
        System.out.println(evaluate);

    }
}
