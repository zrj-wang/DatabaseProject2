package io.sustc.service.impl;

import io.sustc.dto.RegisterUserReq;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//jz-gong
public class UserServiceImpl {
    public long register(RegisterUserReq req){
        if (req.getPassword() == null || req.getPassword().isEmpty() ||
                req.getName() == null || req.getName().isEmpty() ||
                req.getSex() == null ||
                (req.getBirthday() != null && !isValidBirthday(req.getBirthday()))) {
            return -1;
        }


    }
    private boolean isValidBirthday(String birthday) {
        // 实现生日有效性检查，确保符合X月X日的格式
            if (birthday == null || birthday.isEmpty()) {
                return false;
            }

            // 正则表达式匹配格式 "X月Y日"
            String regex = "^([1-9]|1[0-2])月([1-9]|[12][0-9]|3[01])日$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(birthday);

            if (!matcher.find()) {
                return false;
            }

            int month = Integer.parseInt(matcher.group(1));
            int day = Integer.parseInt(matcher.group(2));

            // 检查2月份日期是否合法（1-29天）
            if (month == 2 && day > 29) {
                return false;
            }

            // 检查4、6、9、11月份的日期是否超过30天
            if ((month == 4 || month == 6 || month == 9 || month == 11) && day > 30) {
                return false;
            }

            // 其他月份的日期不能超过31天
            if (day > 31) {
                return false;
            }

            return true;
        }

}
