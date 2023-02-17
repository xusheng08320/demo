package com.xusheng.mockito.service;

import com.xusheng.mockito.util.CommonUtil;
import javafx.scene.control.RadioButton;
import javafx.scene.control.RadioMenuItem;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.*;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

class DemoServiceTest {

    @Test
    public void testRandom() {
        Random random = Mockito.mock(Random.class);
        System.out.println(random.nextInt());
        Mockito.verify(random, Mockito.times(1)).nextInt();
    }

    @Test
    public void testRandom1() {
        Random random = Mockito.mock(Random.class);
        Mockito.when(random.nextInt()).thenReturn(100);
        Assertions.assertEquals(100, random.nextInt());
    }


    @Test
    void add() {
        try (MockedStatic<CommonUtil> commonUtilMockedStatic = Mockito.mockStatic(CommonUtil.class)) {
            commonUtilMockedStatic.when(() -> CommonUtil.sub(ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenReturn(3);
            Assertions.assertEquals(3, CommonUtil.sub(5, 3));
        }
    }

    @Test
    void sub() {
    }
}