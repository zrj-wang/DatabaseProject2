package io.sustc.service.impl;
import io.sustc.dto.AuthInfo;

import io.sustc.service.DanmuService;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.ArrayList;

import java.util.List;
//zrj-wang

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;


//    private ExecutorService executorService;


//    @PostConstruct
//    public void init() {
//        executorService = Executors.newFixedThreadPool(10);
//    }
//
//    private Cache<Long, Boolean> authCache = CacheBuilder.newBuilder()
//            .maximumSize(1000)
//            .expireAfterWrite(30, TimeUnit.MINUTES)
//            .build();//创建缓存实例
//
//    private Cache<WatchedKey, Boolean> watchedVideoCache = CacheBuilder.newBuilder()
//            .maximumSize(1000)
//            .expireAfterWrite(1, TimeUnit.HOURS)
//            .build();

//    private static class WatchedKey {
//        private final long mid;
//        private final String bv;
//
//        WatchedKey(long mid, String bv) {
//            this.mid = mid;
//            this.bv = bv;
//        }
//
//        @Override
//        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//            WatchedKey that = (WatchedKey) o;
//            return mid == that.mid && Objects.equals(bv, that.bv);
//        }
//
//        @Override
//        public int hashCode() {
//            return Objects.hash(mid, bv);
//        }
//    }


    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {

        Long userMid = auth.getMid();

        if (userMid == 0 && (auth.getQq() != null || auth.getWechat() != null)) {
            userMid = getMidFromAuthInfo(auth);
            if (userMid == null) {
                return -1;
            }
        }


        if(!validtime(time,bv)){
            return -1;
        }


//        Future<Long> future = executorService.submit(() -> {
            if ( !isValidAuth(auth)) {

                return -1L;
            }

            if ( !videoExists(bv)) {

                return -1L;
            }

            if (content == null || content.trim().isEmpty()) {

                return -1L;
            }

            if (!isVideoPublished(bv)) {

                return -1L;
            }

            if (!hasWatchedVideo(userMid, bv)) {

                return -1L;
            }

            return insertDanmu(bv, userMid, content, time);
    }
//        });
//
//        try {
//
//            return future.get();
//        } catch (InterruptedException  | ExecutionException e) {
//            log.error("Error occurred while sending danmu", e);
//            Thread.currentThread().interrupt();
//
//            return -1;
//        }


    private boolean validtime(float time,String BV) {
        String sql = "SELECT duration FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, BV);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                float duration = rs.getFloat("duration");
                return time >= 0 && time <= duration;
            }
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
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



    private boolean videoExists(String bv) {

        // 直接检查bv是否为空，避免不必要的数据库操作
        if (bv == null || bv.trim().isEmpty()) {
            return false;
        }
        String sql = "SELECT COUNT(*) FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, bv);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }



    public boolean hasWatchedVideo(long mid, String bv) {

        // 检查参数是否有效
        if (mid <= 0 || bv == null || bv.trim().isEmpty()) {
            return false;
        }
        String sql = "SELECT COUNT(*) FROM watched_relation WHERE user_watched_Mid = ? AND video_watched_BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, mid);
            pstmt.setString(2, bv);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;

    }



    private boolean isVideoPublished(String bv) {

        // 检查bv是否有效
        if (bv == null || bv.trim().isEmpty()) {
            return false;
        }
        String sql = "SELECT public_time, review_time FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, bv);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Timestamp publicTime = rs.getTimestamp("public_time");
                Timestamp reviewTime = rs.getTimestamp("review_time");

                // 检查public_time是否存在且在review_time之后
                return publicTime != null && (reviewTime == null || publicTime.after(reviewTime)||publicTime.equals(reviewTime));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return false;
    }




    private long insertDanmu(String bv, long mid, String content, float time) {


        String sql = "INSERT INTO danmu (danmu_BV, danmu_Mid, time, content, postTime) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {

            pstmt.setString(1, bv);
            pstmt.setLong(2, mid);
            pstmt.setFloat(3, time);
            pstmt.setString(4, content);
            pstmt.executeUpdate();
            ResultSet generatedKeys = pstmt.getGeneratedKeys();

                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                } else {
                    return -1;
                }


        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }





    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {



//        Future<List<Long>> future = executorService.submit(() -> {
            if (bv == null || bv.trim().isEmpty() || timeStart < 0 || timeEnd < 0 || timeStart > timeEnd) {
                return null;
            }

            if(!videoExists(bv)){
                return null;
            }

            if (!isVideoPublished(bv) || !isValidTimeRange(bv, timeStart, timeEnd)) {
                return null;
            }

            String sql = buildDanmuQuerySql(bv, timeStart, timeEnd, filter);
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, bv);
                pstmt.setFloat(2, timeStart);
                pstmt.setFloat(3, timeEnd);
                ResultSet rs = pstmt.executeQuery();
                List<Long> danmuIds = new ArrayList<>();
                while (rs.next()) {
                    danmuIds.add(rs.getLong("danmu_id"));
                }
                return danmuIds;
            } catch (SQLException e) {
                e.printStackTrace();
            }

            return null;



//        });



    }



    private boolean isValidTimeRange(String bv, float timeStart, float timeEnd) {
        // 参数检查
        if (bv == null || timeStart < 0 || timeEnd < 0 || timeEnd < timeStart) {
            return false;
        }
        String sql = "SELECT duration FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, bv);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                float duration = rs.getFloat("duration");
                return timeStart >= 0 && timeEnd <= duration && timeStart <= timeEnd;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private String buildDanmuQuerySql(String bv, float timeStart, float timeEnd, boolean filter) {
        String baseSql = "SELECT danmu_id FROM danmu WHERE danmu_BV = ? AND time >= ? AND time <= ?";
        if (filter) {
            return baseSql + " GROUP BY danmu_id,content ORDER BY MIN(postTime), time";
        } else {
            return baseSql + " ORDER BY time";
        }
    }




    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {



//        Future<Boolean> future = executorService.submit(() -> {
            if (auth == null || id <= 0) {
                return false;
            }
            if (!isValidAuth(auth)) {
                return false;
            }
        Long userMid = auth.getMid();

        if (userMid == 0 && (auth.getQq() != null || auth.getWechat() != null)) {
            userMid = getMidFromAuthInfo(auth);
            if (userMid == null) {
                return false;
            }
        }

            // 检查弹幕是否存在
            if (!danmuExists(id)) {
                return false;
            }

            String sql = "SELECT COUNT(*) FROM danmuLiked_relation WHERE danmu_liked_id = ? AND user_liked_Mid = ?";
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setLong(1, id);
                pstmt.setLong(2, userMid);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) {
                    boolean isLiked = rs.getInt(1) > 0;

                    if (isLiked) {
                        // 如果已点赞，取消点赞
                        return removeLike(conn, id, auth.getMid());
                    } else {
                        // 如果未点赞，添加点赞
                        return addLike(conn, id, auth.getMid());
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return false;

//        });

//        try {
//            return future.get();
//        } catch (InterruptedException | ExecutionException e) {
//            log.error("Error occurred when likeDanmu", e);
//            Thread.currentThread().interrupt(); // 重置中断状态
//            return false;
//        }


    }


    private boolean danmuExists(long id) {
        String sql = "SELECT COUNT(*) FROM danmu WHERE danmu_id = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, id);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean addLike(Connection conn, long id, long userMid) {
        if (id <= 0 || userMid <= 0) {
            return false;
        }
        String sql = "INSERT INTO danmuLiked_relation (danmu_liked_id, user_liked_Mid) VALUES (?, ?)";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, id);
            pstmt.setLong(2, userMid);
            int rowsAffected = pstmt.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    private boolean removeLike(Connection conn, long id, long userMid) {
        if (id <= 0 || userMid <= 0) {
            return false;
        }
        String sql = "DELETE FROM danmuLiked_relation WHERE danmu_liked_id = ? AND user_liked_Mid = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, id);
            pstmt.setLong(2, userMid);
            int rowsAffected = pstmt.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
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
