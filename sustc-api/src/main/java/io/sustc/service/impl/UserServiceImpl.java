package io.sustc.service.impl;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.RegisterUserReq;
import io.sustc.dto.UserInfoResp;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//jz-gong
@Service
@Slf4j
public class UserServiceImpl implements UserService {
    @Autowired
    private DataSource dataSource;
    private ExecutorService executorService;
//    @PostConstruct
//    public void init() {
//        executorService = Executors.newFixedThreadPool(10);
//    }

    public long register(RegisterUserReq req){

        if (req.getPassword() == null || req.getPassword().isEmpty() ||
                req.getName() == null || req.getName().isEmpty() ||
                req.getSex() == null ||
                (req.getBirthday() != null && !isValidBirthday(req.getBirthday()))) {
            return -1;
        }
        // 检查是否存在重复的用户名、QQ号或微信号
        if (isUserExist(req.getQq(), req.getWechat())) {
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
    private boolean isUserExist(String qq, String wechat) {



        // 构建SQL查询语句，检查是否存在具有相同用户名、QQ号或微信号的用户
        String sql = "SELECT COUNT(*) FROM users WHERE  qq = ? OR wechat = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置SQL查询参数

            pstmt.setString(1, qq);
            pstmt.setString(2, wechat);

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

        // 对用户密码使用 SHA-256 加密
        String encryptedPassword = hashPasswordWithSHA256(req.getPassword());

        // SQL 插入语句，包括了默认值设置
        String sql = "INSERT INTO Users (mid, name, sex, birthday, level, coin, identity, sign, password, qq, wechat) " +
                "VALUES (?, ?, ?, ?, 0, 0, 'USER', ?, ?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            // 设置 SQL 参数
            pstmt.setLong(1, mid);
            pstmt.setString(2, req.getName());
            pstmt.setString(3, convertGender(req.getSex()));
            pstmt.setString(4, req.getBirthday());
            pstmt.setString(5, req.getSign());
            pstmt.setString(6, encryptedPassword); // 使用加密后的密码
            pstmt.setString(7, req.getQq());
            pstmt.setString(8, req.getWechat());

            // 执行 SQL 语句
            int affectedRows = pstmt.executeUpdate();
            if (affectedRows > 0) {
                return mid; // 成功插入后返回生成的 mid
            } else {
                return -1; // 如果没有插入行，则返回 -1
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return -1; // 如果发生错误或无法创建用户，则返回 -1
        }
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
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        Long userid;
        if(auth.getMid() != 0){
            userid = auth.getMid();
        } else {
            // 验证 auth 是否有效，并从中获取用户的 mid
            userid = getMidFromAuthInfo(auth);
            if (userid == null) {
                return false; // 用户认证无效或无法获取用户 ID
            }
        }


            // 执行删除操作
            return performDeleteAccount(userid,mid);

    }

    public boolean performDeleteAccount(Long userid, long mid) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmtCheckUser = conn.prepareStatement("SELECT identity FROM users WHERE mid = ?")) {

            // 检查当前用户权限
            pstmtCheckUser.setLong(1, userid);
            try (ResultSet rs = pstmtCheckUser.executeQuery()) {
                if (!rs.next()) {
                    return false; // 当前用户不存在
                }
                String identity = rs.getString("identity");

                if ("USER".equals(identity) && userid != mid) {
                    return false; // 普通用户不能删除其他用户
                }
                if ("SUPERUSER".equals(identity) && userid != mid) {
                    // 检查要删除的用户是否存在
                    try (PreparedStatement pstmtCheckTargetUser = conn.prepareStatement("SELECT identity FROM users WHERE mid = ?")) {
                        pstmtCheckTargetUser.setLong(1, mid);
                        try (ResultSet rsTargetUser = pstmtCheckTargetUser.executeQuery()) {
                            if (!rsTargetUser.next() || "SUPERUSER".equals(rsTargetUser.getString("identity"))) {
                                return false; // 超级用户不能删除其他超级用户或不存在的用户
                            }
                        }
                    }
                }
            }

            // 删除用户相关的所有数据
            String[] sqlCommands = {
                    "DELETE FROM users WHERE mid = ?",
                    "DELETE FROM danmu WHERE danmu_Mid = ?",
                    "DELETE FROM danmuliked_relation WHERE user_liked_Mid = ?",
                    "DELETE FROM following_relation WHERE user_Mid = ? OR follow_Mid = ?",
                    "DELETE FROM liked_relation WHERE user_like_Mid = ?",
                    "DELETE FROM coin_relation WHERE user_coin_Mid = ?",
                    "DELETE FROM watched_relation WHERE user_watched_Mid = ?",
                    "DELETE FROM favorite_relation WHERE user_favorite_Mid = ?",
                    "DELETE FROM videos WHERE owner_Mid = ?"
            };

            for (String sql : sqlCommands) {
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setLong(1, mid);
                    if (sql.contains("following_relation")) {
                        pstmt.setLong(2, mid); // 专门为 following_relation 表设置第二个参数
                    }
                    pstmt.executeUpdate();
                }
            }

            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }


    private Long getMidFromAuthInfo(AuthInfo auth) {
        String sql = null;
        ResultSet rs = null;

        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement pstmt;

            if (auth.getQq() != null) {
                sql = "SELECT mid FROM Users WHERE qq = ?";
                pstmt = conn.prepareStatement(sql);
                pstmt.setString(1, auth.getQq());
            } else if (auth.getWechat() != null) {
                sql = "SELECT mid FROM Users WHERE wechat = ?";
                pstmt = conn.prepareStatement(sql);
                pstmt.setString(1, auth.getWechat());
            } else {
                return null; // 没有足够的认证信息
            }

            rs = pstmt.executeQuery();

            if (rs.next()) {
                return rs.getLong("mid");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }


    private boolean isValidAuth(AuthInfo auth) {
        String sql = null;
        ResultSet rs = null;

        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement pstmt;

            if (auth.getMid() != 0 && auth.getPassword() != null) {
                // 情况1：提供了 mid 和密码
                sql = "SELECT password FROM Users WHERE mid = ?";
                pstmt = conn.prepareStatement(sql);
                pstmt.setLong(1, auth.getMid());
            } else if (auth.getQq() != null) {
                // 情况2：仅提供了 qq
                sql = "SELECT COUNT(*) FROM Users WHERE qq = ?";
                pstmt = conn.prepareStatement(sql);
                pstmt.setString(1, auth.getQq());
            } else if (auth.getWechat() != null) {
                // 情况3：仅提供了 wechat
                sql = "SELECT COUNT(*) FROM Users WHERE wechat = ?";
                pstmt = conn.prepareStatement(sql);
                pstmt.setString(1, auth.getWechat());
            } else {
                return false; // 没有提供足够的认证信息
            }

            rs = pstmt.executeQuery();

            if (rs.next()) {
                if (auth.getMid() != 0 && auth.getPassword() != null) {
                    // 验证密码
                    String storedPassword = rs.getString("password");
                    String encryptedInputPassword = hashPasswordWithSHA256(auth.getPassword());
                    return encryptedInputPassword.equals(storedPassword);
                } else {
                    // 对于 qq 或 wechat，检查用户是否存在
                    int count = rs.getInt(1);
                    return count > 0;
                }
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








    public boolean follow(AuthInfo auth, long followeeMid) {
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        Long userid;
        if (auth.getMid() != 0) {
            userid = auth.getMid();
        } else {
            // 验证 auth 是否有效，并从中获取用户的 mid
            userid = getMidFromAuthInfo(auth);
            if (userid == null) {
                return false; // 用户认证无效或无法获取用户 ID
            }
        }
        if (userid == followeeMid) {
            return false; // 用户不能关注自己
        }

        try (Connection conn = dataSource.getConnection();
             PreparedStatement checkUserStmt = conn.prepareStatement("SELECT COUNT(*) FROM users WHERE mid = ?")) {

            // 检查被关注的用户是否存在
            checkUserStmt.setLong(1, followeeMid);
            try (ResultSet rs = checkUserStmt.executeQuery()) {
                if (!(rs.next() && rs.getInt(1) > 0)) {
                    return false; // 被关注的用户不存在
                }
            }

            // 检查当前用户是否已关注目标用户
            try (PreparedStatement checkFollowStmt = conn.prepareStatement("SELECT COUNT(*) FROM following_relation WHERE user_Mid = ? AND follow_Mid = ?")) {
                checkFollowStmt.setLong(1, userid);
                checkFollowStmt.setLong(2, followeeMid);
                try (ResultSet rs = checkFollowStmt.executeQuery()) {
                    if (rs.next() && rs.getInt(1) > 0) {
                        // 如果已经关注，则取消关注
                        try (PreparedStatement deleteStmt = conn.prepareStatement("DELETE FROM following_relation WHERE user_Mid = ? AND follow_Mid = ?")) {
                            deleteStmt.setLong(1, userid);
                            deleteStmt.setLong(2, followeeMid);
                            deleteStmt.executeUpdate();
                            return false; // 关注已取消
                        }
                    } else {
                        // 如果尚未关注，则添加关注
                        try (PreparedStatement insertStmt = conn.prepareStatement("INSERT INTO following_relation (user_Mid, follow_Mid) VALUES (?, ?)")) {
                            insertStmt.setLong(1, userid);
                            insertStmt.setLong(2, followeeMid);
                            insertStmt.executeUpdate();
                            return true; // 关注成功
                        }
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }



    public UserInfoResp getUserInfo(long mid) {
        // 使用提供的 userExists 方法来检查用户是否存在
        if (!userExists(mid)) {
            return null; // 如果用户不存在，直接返回 null
        }

        UserInfoResp userInfo = new UserInfoResp();
        userInfo.setMid(mid);

        try (Connection conn = dataSource.getConnection()) {
            // 填充用户信息
            userInfo.setCoin(getUserCoin(conn, mid));
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
        String sql = "SELECT video_watched_BV FROM watched_relation WHERE user_watched_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<String> watchedList = new ArrayList<>();
            while (rs.next()) {
                watchedList.add(rs.getString("video_watched_BV"));
            }
            return watchedList.toArray(new String[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new String[0]; // 如果找不到用户或发生异常，返回空数组
    }
    private String[] getLikedVideos(Connection conn, long mid) {
        String sql = "SELECT video_like_bv FROM liked_relation WHERE user_like_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<String> likedList = new ArrayList<>();
            while (rs.next()) {
                likedList.add(rs.getString("video_like_BV"));
            }
            return likedList.toArray(new String[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new String[0]; // 如果找不到用户或发生异常，返回空数组
    }
    private String[] getCollectedVideos(Connection conn, long mid) {
        String sql = "SELECT video_favorite_BV FROM favorite_relation WHERE user_favorite_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, mid);
            ResultSet rs = pstmt.executeQuery();

            ArrayList<String> collectedList = new ArrayList<>();
            while (rs.next()) {
                collectedList.add(rs.getString("video_favorite_BV"));
            }
            return collectedList.toArray(new String[0]);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new String[0]; // 如果找不到用户或发生异常，返回空数组
    }
    private String[] getPostedVideos(Connection conn, long mid) {
        String sql = "SELECT BV FROM videos WHERE owner_Mid = ?";
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
    private String hashPasswordWithSHA256(String password) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(password.getBytes());

            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b));
            }

            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to hash password", e);
        }
    }





}
