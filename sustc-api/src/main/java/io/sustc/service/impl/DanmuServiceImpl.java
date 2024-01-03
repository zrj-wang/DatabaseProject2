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
import java.util.concurrent.*;


import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;


    private ExecutorService executorService;


    private Cache<String, Boolean> watchedVideoCache;
    @PostConstruct
    public void init() {
        executorService = Executors.newFixedThreadPool(8);

        watchedVideoCache = CacheBuilder.newBuilder()
                .maximumSize(1000) // 最大缓存项数
                .expireAfterAccess(30, TimeUnit.MINUTES) // 30分钟后过期
                .build();


    }




    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {


        Future<Long> future = executorService.submit(() -> {

            if ( !isValidAuth(auth)) {
                return -1L;
            }

            Long userMid = auth.getMid();

            if (userMid == 0 && (auth.getQq() != null || auth.getWechat() != null)) {
                userMid = getMidFromAuthInfo(auth);
                if (userMid == null) {
                    return -1L;
                }
            }


            if(!validtime(time,bv)){
                return -1L;
            }



            if ( !videoExists(bv)) {

                return -1L;
            }

            if (content == null || content.isEmpty()) {

                return -1L;
            }

            if (!isVideoPublished(bv)) {

                return -1L;
            }

            if (!hasWatchedVideo(userMid, bv)) {

                return -1L;
            }

            return insertDanmu(bv, userMid, content, time);

        });

        try {

            return future.get();
        } catch (InterruptedException  | ExecutionException e) {
            log.error("Error occurred while sending danmu", e);
            Thread.currentThread().interrupt();

            return -1;
        }
        }



//
//    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
//        if(auth==null){
//            return -1L;
//        }
//
//        AtomicReference<Long> result = new AtomicReference<>(-1L);
//        long mid = auth.getMid();
//        if (auth.getMid() == 0 && (auth.getQq() != null || auth.getWechat() != null)) {
//            mid = getMidFromAuthInfo(auth);
//
//        }
//
//        long finalMid = mid;
//        CompletableFuture<Void> allOf = CompletableFuture.allOf(
//                CompletableFuture.supplyAsync(() -> isValidAuth(auth), executorService).thenAcceptAsync(authResult -> {
//                    if (!authResult) {
//                        result.set(-1L);
//                    }
//                }),
//                CompletableFuture.supplyAsync(() -> validtime(time, bv), executorService).thenAcceptAsync(validTimeResult -> {
//                    if (!validTimeResult) {
//                        result.set(-1L);
//                    }
//                }),
//                CompletableFuture.supplyAsync(() -> videoExists(bv), executorService).thenAcceptAsync(videoExistsResult -> {
//                    if (!videoExistsResult) {
//                        result.set(-1L);
//                    }
//                }),
//                CompletableFuture.supplyAsync(() -> isVideoPublished(bv), executorService).thenAcceptAsync(videoPublishedResult -> {
//                    if (!videoPublishedResult) {
//                        result.set(-1L);
//                    }
//                }),
//                CompletableFuture.supplyAsync(() -> hasWatchedVideo(finalMid, bv), executorService).thenAcceptAsync(watchedVideoResult -> {
//                    if (!watchedVideoResult) {
//                        result.set(-1L);
//                    }
//                })
//        );
//
//        // 等待所有小方法执行完成
//        allOf.join();
//
//        // 检查是否有小方法返回 -1
//        if (result.get() == -1L) {
//            return -1L;
//        }
//
//        // 单独执行 insertDanmu
//        return insertDanmu(bv, finalMid, content, time);
//    }



    private boolean validtime(float time,String BV) {
        String sql = "SELECT duration FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, BV);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                float duration = rs.getFloat("duration");
                return time < duration;
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




//
//    public boolean hasWatchedVideo(long mid, String bv) {
//        // 检查参数是否有效
//        if (mid <= 0 || bv == null || bv.trim().isEmpty()) {
//            return false;
//        }
//
//        // 创建缓存键
//        String key = "watched:" + mid + ":" + bv;
//        Boolean result = watchedVideoCache.getIfPresent(key);
//
//        // 检查缓存中是否有结果
//        if (result != null) {
//            return result;
//        }
//
//        // 缓存中没有结果，执行数据库查询
//        String sql = "SELECT COUNT(*) FROM watched_relation WHERE user_watched_Mid = ? AND video_watched_BV = ?";
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement pstmt = conn.prepareStatement(sql)) {
//
//            pstmt.setLong(1, mid);
//            pstmt.setString(2, bv);
//            ResultSet rs = pstmt.executeQuery();
//            if (rs.next()) {
//                boolean hasWatched = rs.getInt(1) > 0;
//                watchedVideoCache.put(key, hasWatched); // 将结果存储到缓存中
//                return hasWatched;
//            }
//        } catch (SQLException e) {
//            log.error("SQL Exception in hasWatchedVideo", e);
//        }
//
//        return false;
//    }



    private boolean isVideoPublished(String bv) {

        // 检查bv是否有效
        if (bv == null || bv.isEmpty()) {
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
//&& (reviewTime == null || publicTime.after(reviewTime)||publicTime.equals(reviewTime))
                // 检查public_time是否存在且在review_time之后,貌似不用检查？？？
                return publicTime != null ;
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
            conn.commit();

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
            //    System.out.println("nodanmu");
                return false;
            }
            String sql = "SELECT COUNT(*) FROM danmuLiked_relation WHERE danmu_liked_id = ? AND user_liked_Mid = ?";
        String sql1 = "INSERT INTO danmuLiked_relation (danmu_liked_id, user_liked_Mid) VALUES (?, ?)";
        String sql2 = "DELETE FROM danmuLiked_relation WHERE danmu_liked_id = ? AND user_liked_Mid = ?";
        String sql3= "select danmu_bv from danmu where danmu_id=?";

        String bv="";
        try (Connection conn1 = dataSource.getConnection();

             PreparedStatement pstmt3 = conn1.prepareStatement(sql3)) {
    pstmt3.setLong(1, id);
    ResultSet rs3 = pstmt3.executeQuery();

    if (rs3.next()) {
        bv=rs3.getString("danmu_bv");

    }


        }catch (SQLException e) {
            e.printStackTrace();
        }

        if(!hasWatchedVideo(userMid,bv)){
            return false;
        }



        try (Connection conn = dataSource.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql);
        PreparedStatement pstmt1 = conn.prepareStatement(sql1);
             PreparedStatement pstmt2 = conn.prepareStatement(sql2);
         ) {
                pstmt.setLong(1, id);
                pstmt.setLong(2, userMid);
                ResultSet rs = pstmt.executeQuery();

            pstmt1.setLong(1, id);
            pstmt1.setLong(2, userMid);

            pstmt2.setLong(1, id);
            pstmt2.setLong(2, userMid);


            if (rs.next()) {
                    boolean isLiked = rs.getInt(1) > 0;


                    if (isLiked) {
                        // 如果已点赞，取消点赞

                        int rowsAffected = pstmt2.executeUpdate();

                   //     conn.commit();
                        //要false
                        return rowsAffected <= 0;

                    } else {
                        // 如果未点赞，添加点赞

                        int rowsAffected = pstmt1.executeUpdate();
                       // conn.commit();
                        return rowsAffected > 0;
                    }
                }
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            System.out.println("final:false");
            return false;
    }


//        });

//        try {
//            return future.get();
//        } catch (InterruptedException | ExecutionException e) {
//            log.error("Error occurred when likeDanmu", e);
//            Thread.currentThread().interrupt(); // 重置中断状态
//            return false;
//        }





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
