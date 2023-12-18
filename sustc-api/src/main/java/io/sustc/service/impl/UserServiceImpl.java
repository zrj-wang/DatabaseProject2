package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


//jz-gong
public class UserServiceImpl {
    @Autowired
    private DataSource dataSource;
    public long register(RegisterUserReq req){
        if (req.getPassword() == null || req.getPassword().isEmpty() ||
                req.getName() == null || req.getName().isEmpty() ||
                req.getSex() == null ||
                (req.getBirthday() != null && !isValidBirthday(req.getBirthday()))) {
            return -1;
        }
        // 检查是否存在重复的用户名、QQ号或微信号
        if (isUserExist(req.getName(), req.getQq(), req.getWechat())) {
            return -1;
        }

        // 添加新用户到系统（假设addNewUser方法负责添加用户并返回用户ID）
        return addNewUser(req);
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
    private boolean isUserExist(String name, String qq, String wechat) {
        // 构建SQL查询语句，检查是否存在具有相同用户名、QQ号或微信号的用户
        String sql = "SELECT COUNT(*) FROM users WHERE name = ? OR qq = ? OR wechat = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置SQL查询参数
            pstmt.setString(1, name);
            pstmt.setString(2, qq);
            pstmt.setString(3, wechat);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                // 检查查询结果
                int count = rs.getInt(1);
                return count > 0; // 如果计数大于0，表示存在重复用户
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 根据您的错误处理策略，您可能想在这里添加更多的错误处理逻辑
        }
        return false; // 如果查询不到结果或发生异常，则假定不存在重复用户
    }
    private long addNewUser(RegisterUserReq req) {
        // 首先生成一个唯一的 mid
        long mid = generateUniqueMid();

        // SQL 插入语句
        String sql = "INSERT INTO Users (mid, name, sex, birthday, sign, password, qq, wechat) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置 SQL 参数
            pstmt.setLong(1, mid);
            pstmt.setString(2, req.getName());
            pstmt.setString(3, convertGender(req.getSex()));
            pstmt.setString(4, req.getBirthday());
            pstmt.setString(5, req.getSign());
            pstmt.setString(6, req.getPassword());
            pstmt.setString(7, req.getQq());
            pstmt.setString(8, req.getWechat());

            // 执行 SQL 语句
            int affectedRows = pstmt.executeUpdate();
            if (affectedRows > 0) {
                return mid; // 成功插入后返回生成的 mid
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1; // 如果发生错误或无法创建用户，则返回 -1
    }


    private long generateUniqueMid() {
        long mid;
        Random random = new Random();
        do {
            // 生成随机的 long 类型的 mid
            mid = random.nextLong();

            // 确保 mid 是正数
            if (mid < 0) {
                mid = -mid;
            }

            // 检查生成的 mid 是否重复
        } while (isMidExist(mid));

        return mid;
    }
    private boolean isMidExist(long mid) {
        String sql = "SELECT COUNT(*) FROM Users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, mid);

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0; // 如果计数大于0，表示存在重复的 mid
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }
    // 将 Gender 枚举转换为对应的数据库字符串
    private String convertGender(RegisterUserReq.Gender gender) {
        if (gender == null) {
            return "保密"; // 或根据需要设置默认值
        }
        switch (gender) {
            case MALE:
                return "男";
            case FEMALE:
                return "女";
            default:
                return "保密";
        }
    }
    public boolean deleteAccount(AuthInfo auth, long mid) {
        try (Connection conn = dataSource.getConnection()) {
            // 验证 auth 是否有效
            if (!isValidAuth(auth)) {
                return false;
            }

            // 检查要删除的用户是否存在
            if (!userExists(mid)) {
                return false;
            }

            // 获取当前用户的类型和 ID
            String currentUserType = getUserType(auth.getMid());
            if (currentUserType == null) {
                return false;
            }

            // 检查权限
            if ("USER".equals(currentUserType) && auth.getMid() != mid) {
                return false;
            }
            if ("SUPERUSER".equals(currentUserType) && auth.getMid() != mid && !isRegularUser(mid)) {
                return false;
            }

            // 执行删除操作
            return deleteUser(mid);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }
    private boolean isValidAuth(AuthInfo auth) {
        // 确保至少提供了一个认证信息
        if (auth.getPassword() == null && auth.getQq() == null && auth.getWechat() == null) {
            return false;
        }

        String sql = "SELECT password, qq, wechat FROM Users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, auth.getMid());
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                String storedPassword = rs.getString("password");

                // 检查密码是否匹配
                boolean isPasswordValid = auth.getPassword() != null && auth.getPassword().equals(storedPassword);
                if (!isPasswordValid) {
                    return false;
                }

                String storedQQ = rs.getString("qq");
                String storedWechat = rs.getString("wechat");

                boolean isQQValid = auth.getQq() == null || auth.getQq().equals(storedQQ);
                boolean isWechatValid = auth.getWechat() == null || auth.getWechat().equals(storedWechat);

                return isQQValid && isWechatValid;
            } else {
                // mid不存在，检查qq和wechat
                return checkQQWechat(auth);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean checkQQWechat(AuthInfo auth) {
        if (auth.getQq() != null && auth.getWechat() != null) {
            // 检查是否存在一个用户同时拥有这个qq和wechat
            return checkUserWithBoth(auth.getQq(), auth.getWechat());
        } else if (auth.getQq() != null) {
            // 检查是否存在拥有这个qq的用户
            return checkUserWithQQ(auth.getQq());
        } else if (auth.getWechat() != null) {
            // 检查是否存在拥有这个wechat的用户
            return checkUserWithWechat(auth.getWechat());
        }
        return false;
    }

    // 实现 checkUserWithQQ, checkUserWithWechat, checkUserWithBoth 方法来检查数据库
    private boolean checkUserWithQQ(String qq) {
        String sql = "SELECT COUNT(*) FROM Users WHERE qq = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, qq);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean checkUserWithWechat(String wechat) {
        String sql = "SELECT COUNT(*) FROM Users WHERE wechat = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, wechat);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean checkUserWithBoth(String qq, String wechat) {
        String sql = "SELECT COUNT(*) FROM Users WHERE qq = ? AND wechat = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, qq);
            pstmt.setString(2, wechat);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }



    private boolean userExists(long mid) {
        // 检查 mid 是否有效
        if (mid <= 0) {
            return false;
        }

        String sql = "SELECT COUNT(*) FROM Users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setLong(1, mid);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();
            if (rs.next() && rs.getInt(1) == 1) {
                // 如果找到对应的用户，返回 true
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 如果没有找到对应的用户或发生异常，返回 false
        return false;
    }


    private String getUserType(long mid) {
        // 检查 mid 是否有效
        if (mid <= 0) {
            return null;
        }

        String sql = "SELECT identity FROM Users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setLong(1, mid);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                // 返回查询到的用户类型
                return rs.getString("identity");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 如果没有找到用户或发生异常，返回 null
        return null;
    }


    private boolean isRegularUser(long mid) {
        // 检查 mid 是否有效
        if (mid <= 0) {
            return false;
        }

        String sql = "SELECT identity FROM Users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setLong(1, mid);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                // 检查用户类型是否为 "USER"
                return "USER".equals(rs.getString("identity"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 如果没有找到用户或发生异常，返回 false
        return false;
    }


    private boolean deleteUser(long mid) {
        // 检查 mid 是否有效
        if (mid <= 0) {
            return false;
        }

        String sql = "DELETE FROM Users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置删除操作的参数
            pstmt.setLong(1, mid);

            // 执行删除操作
            int rowsAffected = pstmt.executeUpdate();
            return rowsAffected > 0; // 如果至少有一行被删除，则返回 true
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 如果没有删除成功或发生异常，返回 false
        return false;
    }
    public boolean follow(AuthInfo auth, long followeeMid) {
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        // 检查被关注的用户是否存在
        if (!userExists(followeeMid)) {
            return false;
        }

        try (Connection conn = dataSource.getConnection()) {
            // 检查当前用户是否已关注目标用户
            String checkSql = "SELECT COUNT(*) FROM following_relation WHERE user_Mid = ? AND follow_Mid = ?";
            try (PreparedStatement checkStmt = conn.prepareStatement(checkSql)) {
                checkStmt.setLong(1, auth.getMid());
                checkStmt.setLong(2, followeeMid);

                ResultSet rs = checkStmt.executeQuery();
                if (rs.next() && rs.getInt(1) > 0) {
                    // 如果已经关注，则取消关注
                    String deleteSql = "DELETE FROM following_relation WHERE user_Mid = ? AND follow_Mid = ?";
                    try (PreparedStatement deleteStmt = conn.prepareStatement(deleteSql)) {
                        deleteStmt.setLong(1, auth.getMid());
                        deleteStmt.setLong(2, followeeMid);
                        deleteStmt.executeUpdate();
                        return false; // 关注已取消
                    }
                } else {
                    // 如果尚未关注，则添加关注
                    String insertSql = "INSERT INTO following_relation (user_Mid, follow_Mid) VALUES (?, ?)";
                    try (PreparedStatement insertStmt = conn.prepareStatement(insertSql)) {
                        insertStmt.setLong(1, auth.getMid());
                        insertStmt.setLong(2, followeeMid);
                        insertStmt.executeUpdate();
                        return true; // 关注成功
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }
    public UserInfoResp getUserInfo(long mid) {
        // 检查 mid 是否有效
        if (mid <= 0) {
            return null;
        }

        UserInfoResp userInfo = new UserInfoResp();
        userInfo.setMid(mid);

        try (Connection conn = dataSource.getConnection()) {
            // 填充 coin
            userInfo.setCoin(getUserCoin(conn, mid));

            // 填充 following, follower, watched, liked, collected, posted
            userInfo.setFollowing(getFollowing(conn, mid));
            userInfo.setFollower(getFollowers(conn, mid));
            userInfo.setWatched(getWatchedVideos(conn, mid));
            userInfo.setLiked(getLikedVideos(conn, mid));
            userInfo.setCollected(getCollectedVideos(conn, mid));
            userInfo.setPosted(getPostedVideos(conn, mid));

            return userInfo; // 返回用户信息
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null; // 如果发生异常，返回 null
    }
    private int getUserCoin(Connection conn, long mid) {
        String sql = "SELECT coin FROM Users WHERE mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("coin");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return 0; // 如果找不到用户或发生异常，返回 0
    }
    private long[] getFollowing(Connection conn, long mid) {
        String sql = "SELECT follow_Mid FROM following_relation WHERE user_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<Long> followingList = new ArrayList<>();
            while (rs.next()) {
                followingList.add(rs.getLong("follow_Mid"));
            }
            return followingList.stream().mapToLong(l -> l).toArray();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new long[0]; // 如果找不到用户或发生异常，返回空数组
    }
    private long[] getFollowers(Connection conn, long mid) {
        String sql = "SELECT user_Mid FROM following_relation WHERE follow_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<Long> followerList = new ArrayList<>();
            while (rs.next()) {
                followerList.add(rs.getLong("user_Mid"));
            }
            return followerList.stream().mapToLong(l -> l).toArray();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new long[0]; // 如果找不到用户或发生异常，返回空数组
    }
    private String[] getWatchedVideos(Connection conn, long mid) {
        String sql = "SELECT bv FROM watched_relation WHERE user_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<String> watchedList = new ArrayList<>();
            while (rs.next()) {
                watchedList.add(rs.getString("bv"));
            }
            return watchedList.toArray(new String[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new String[0]; // 如果找不到用户或发生异常，返回空数组
    }
    private String[] getLikedVideos(Connection conn, long mid) {
        String sql = "SELECT bv FROM liked_relation WHERE user_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<String> likedList = new ArrayList<>();
            while (rs.next()) {
                likedList.add(rs.getString("bv"));
            }
            return likedList.toArray(new String[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new String[0]; // 如果找不到用户或发生异常，返回空数组
    }
    private String[] getCollectedVideos(Connection conn, long mid) {
        String sql = "SELECT bv FROM collected_relation WHERE user_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<String> collectedList = new ArrayList<>();
            while (rs.next()) {
                collectedList.add(rs.getString("bv"));
            }
            return collectedList.toArray(new String[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new String[0]; // 如果找不到用户或发生异常，返回空数组
    }
    private String[] getPostedVideos(Connection conn, long mid) {
        String sql = "SELECT bv FROM posted_relation WHERE user_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<String> postedList = new ArrayList<>();
            while (rs.next()) {
                postedList.add(rs.getString("bv"));
            }
            return postedList.toArray(new String[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new String[0]; // 如果找不到用户或发生异常，返回空数组
    }





}
