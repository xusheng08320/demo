package com.xusheng.mockito.service;

import com.xusheng.mockito.util.CommonUtil;

public class DemoService {

    private CommonUtil commonUtil;

    public int add(int a, int b) {
        return commonUtil.add(a, b);
    }

    public int sub(int a, int b) {
        return CommonUtil.sub(a, b);
    }

}
