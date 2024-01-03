package io.sustc.service.impl;
//zrj-wang

import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DanmuService;
import io.sustc.service.DatabaseService;
import io.sustc.service.RecommenderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Service
@Slf4j
public class RecommenderServiceImpl implements RecommenderService {

    @Autowired
    private DataSource dataSource;
    private ExecutorService executorService;
//    @PostConstruct
//    public void init() {
//        executorService = Executors.newFixedThreadPool(10);
//    }
//

    @Override
    public List<String> recommendNextVideo(String bv) {
        // 创建用于存储推荐视频 bv 的列表
//        Future<List<String>> future = executorService.submit(() -> {
            List<String> recommendedVideos = new ArrayList<>();
            if (bv == null || bv.trim().isEmpty()) {
                return null;
            }
            // 检查视频是否存在
            if (!videoExists(bv)) {
                return null;
            }

            // 查询与当前视频最相似的前5个视频
            String sql = "SELECT video_watched_BV, COUNT(*) AS similarity " +
                    "FROM watched_relation " +
                    "WHERE video_watched_BV != ? " +
                    "AND user_watched_Mid IN (SELECT user_watched_Mid FROM watched_relation WHERE video_watched_BV = ?) " +
                    "GROUP BY video_watched_BV " +
                    "ORDER BY similarity DESC, video_watched_BV ASC " +
                    "LIMIT 5";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setString(1, bv);
                pstmt.setString(2, bv);

                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    String recommendedBV = rs.getString("video_watched_BV");
                    recommendedVideos.add(recommendedBV);
                }

            }catch (SQLException e) {
                e.printStackTrace();
            }


            return recommendedVideos;

//        });

//        try {
//
//            return future.get();
//        } catch (InterruptedException  | ExecutionException e) {
//            log.error("Error occurred while recommendNextVideo", e);
//            Thread.currentThread().interrupt();
//            return null;
//        }


    }

    private boolean videoExists(String bv) {

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





    //wrong
    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {


//        Future<List<String>> future = executorService.submit(() -> {
        // 检查分页参数的有效性
        if (pageSize <= 0 || pageNum <= 0) {
            return null;
        }

        List<String> recommendedVideos = new ArrayList<>();

        String sql = "SELECT v.bv,"
                +"COALESCE(like_rate,  0) AS like_rate,"
        +"COALESCE(LEAST(coin_rate, 1), 0) AS coin_rate,"
        +"COALESCE(LEAST(fav_rate, 1), 0) AS fav_rate,"
        +"COALESCE(danmu_avg, 0) AS danmu_avg,"
        +"COALESCE(finish_avg, 0) AS finish_avg,"
        +"(COALESCE(LEAST(like_rate, 1), 0) + COALESCE(LEAST(coin_rate, 1), 0) + COALESCE(LEAST(fav_rate, 1), 0) + COALESCE(danmu_avg, 0) + COALESCE(finish_avg, 0)) AS score "
        +"FROM videos v "
        +"LEFT JOIN (SELECT video_like_BV, COUNT(*)*1. / NULLIF((SELECT COUNT(*) FROM watched_relation WHERE video_watched_BV = video_like_BV), 0) AS like_rate FROM liked_relation GROUP BY video_like_BV) lr ON v.BV = lr.video_like_BV "
        +"LEFT JOIN (SELECT video_coin_BV, COUNT(*)*1. / NULLIF((SELECT COUNT(*) FROM watched_relation WHERE video_watched_BV = video_coin_BV), 0) AS coin_rate FROM coin_relation GROUP BY video_coin_BV) cr ON v.BV = cr.video_coin_BV "
        +"LEFT JOIN (SELECT video_favorite_BV, COUNT(*) *1./ NULLIF((SELECT COUNT(*) FROM watched_relation WHERE video_watched_BV = video_favorite_BV), 0) AS fav_rate FROM favorite_relation GROUP BY video_favorite_BV) fr ON v.BV = fr.video_favorite_BV "
        +"LEFT JOIN (SELECT danmu_BV, COUNT(*) *1./ NULLIF((select  count(*) from watched_relation where video_watched_BV = danmu_BV),0) as danmu_avg from  danmu group by danmu_BV) dr ON v.BV = dr.danmu_BV "
        +"LEFT JOIN (SELECT video_watched_BV, AVG(watched_time / duration) AS finish_avg FROM watched_relation wr JOIN videos vs ON wr.video_watched_BV = vs.BV GROUP BY video_watched_BV) wr ON v.BV = wr.video_watched_BV "
        +"ORDER BY score DESC "
        +"LIMIT ? OFFSET ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            int offset = (pageNum - 1) * pageSize;
            pstmt.setInt(1, pageSize);
            pstmt.setInt(2, offset);


            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String bv = rs.getString("BV");
                recommendedVideos.add(bv);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return recommendedVideos;

    }
//        });
//
//        try {
//            // 等待异步执行完成并获取结果
//            return future.get();
//        } catch (InterruptedException | ExecutionException e) {
//            // 异常处理
//            log.error("Error occurred when likeDanmu", e);
//            Thread.currentThread().interrupt(); // 重置中断状态
//            return null;
//        }


    private int getTotalVideoCount(Long userMid) {
        int totalVideos = 0;
        String sql = "SELECT COUNT(DISTINCT v.BV) " +
                "FROM videos v " +
                "JOIN (SELECT video_watched_BV FROM watched_relation WHERE user_watched_Mid IN ( " +
                "SELECT fr2.user_Mid FROM following_relation fr1 " +
                "JOIN following_relation fr2 ON fr1.user_Mid = fr2.follow_Mid AND fr1.follow_Mid = fr2.user_Mid " +
                "WHERE fr1.user_Mid = ?) " +
                "AND video_watched_BV NOT IN (SELECT video_watched_BV FROM watched_relation WHERE user_watched_Mid = ?)) watched_videos ON v.BV = watched_videos.video_watched_BV " +
                "JOIN Users u ON v.owner_Mid = u.mid;";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, userMid);
            pstmt.setLong(2, userMid);

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                totalVideos = rs.getInt(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return totalVideos;
    }





    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {

//        Future<List<String>> future = executorService.submit(() -> {
        // 检查认证信息、分页参数的有效性
        if (auth == null || !isValidAuth(auth) || pageSize <= 0 || pageNum <= 0) {
            return null;
        }

        Long userMid = auth.getMid();

        if (userMid == 0 && (auth.getQq() != null || auth.getWechat() != null)) {
            userMid = getMidFromAuthInfo(auth);
            if (userMid == null) {
                return new ArrayList<>();
            }
        }

        int totalVideos = getTotalVideoCount(userMid);
        int totalPages = (int) Math.ceil((double) totalVideos / pageSize);

        if (pageNum > totalPages&&totalPages>=1) {
            return new ArrayList<>();
        }

        List<String> recommendedVideos = new ArrayList<>();
        String sql = "SELECT\n" +
                "    v.BV,\n" +
                "    COUNT(watched_videos.video_watched_BV) AS watched_times,\n" +
                "    u.level,\n" +
                "    v.public_time\n" +
                "FROM videos v\n" +
                "JOIN (\n" +
                "    SELECT video_watched_BV\n" +
                "    FROM watched_relation\n" +
                "    WHERE user_watched_Mid IN (\n" +
                "        SELECT fr2.user_Mid\n" +
                "        FROM following_relation fr1\n" +
                "        JOIN following_relation fr2\n" +
                "        ON fr1.user_Mid = fr2.follow_Mid AND fr1.follow_Mid = fr2.user_Mid\n" +
                "        WHERE fr1.user_Mid = ?\n" +
                "    )\n" +
                "    AND video_watched_BV NOT IN (\n" +
                "        SELECT video_watched_BV\n" +
                "        FROM watched_relation\n" +
                "        WHERE user_watched_Mid = ?\n" +
                "    )\n" +
                ") watched_videos ON v.BV = watched_videos.video_watched_BV\n" +
                "JOIN Users u ON v.owner_Mid = u.mid\n" +
                "GROUP BY v.BV, u.level, v.public_time\n" +
                "ORDER BY watched_times DESC, u.level DESC, v.public_time DESC\n" +
                "LIMIT ? OFFSET ? ";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);

            ) {

            int offset = (pageNum - 1) * pageSize;
            pstmt.setLong(1, userMid);
            pstmt.setLong(2, userMid);
            pstmt.setInt(3, pageSize);
            pstmt.setInt(4, offset);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                String bv = rs.getString("BV");
                recommendedVideos.add(bv);
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }

        if(recommendedVideos.isEmpty()){
            return generalRecommendations(pageSize, pageNum);
        }


        return recommendedVideos;
    }

//        });
//
//        try {
//            // 等待异步执行完成并获取结果
//            return future.get();
//        } catch (InterruptedException | ExecutionException e) {
//            // 异常处理
//            log.error("Error occurred when likeDanmu", e);
//            Thread.currentThread().interrupt(); // 重置中断状态
//            return null;
//        }







//    @Override
//    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
//        // 检查认证信息和分页参数的有效性
//        if (auth == null || !isValidAuth(auth) || pageSize <= 0 || pageNum <= 0) {
//            return null;
//        }
//
//        Long userMid = auth.getMid();
//        if (userMid == 0 && (auth.getQq() != null || auth.getWechat() != null)) {
//            if (getMidFromAuthInfo(auth) == null) {
//                return null;
//            }else {
//                userMid = getMidFromAuthInfo(auth);
//            }
//        }
//
//        //过不了oj 可能是at least？？？先标记一下
////        Future<List<Long>> future = executorService.submit(() -> {
//
//        String sql = "SELECT fr2.user_Mid, COUNT(*) as common_followings, u.level " +
//                "FROM following_relation fr1 " +
//                "JOIN following_relation fr2 ON fr1.follow_Mid = fr2.follow_Mid AND fr1.user_Mid != fr2.user_Mid " +
//                "JOIN Users u ON fr2.user_Mid = u.mid " +
//                "WHERE fr1.user_Mid = ? AND fr2.user_Mid NOT IN (SELECT follow_Mid FROM following_relation WHERE user_Mid = ?) " +
//                "GROUP BY fr2.user_Mid, u.level " +
//                "ORDER BY common_followings DESC, u.level DESC, fr2.user_Mid ASC " +
//                "LIMIT ? OFFSET ?";
////
//
//
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement pstmt = conn.prepareStatement(sql)) {
//            List<Long> recommendedUserIds = new ArrayList<>();
//
//            int offset = (pageNum - 1) * pageSize;
//            pstmt.setLong(1, userMid);
//            pstmt.setLong(2, userMid);
//            pstmt.setInt(3, pageSize);
//            pstmt.setInt(4, offset);
//
//            ResultSet rs = pstmt.executeQuery();
//            while (rs.next()) {
//                Long userId = rs.getLong("user_Mid");
//                recommendedUserIds.add(userId);
//            }
//            return recommendedUserIds;
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//
//    }

//        });

//        try {
//            // 等待异步执行完成并获取结果
//            return future.get();
//        } catch (InterruptedException | ExecutionException e) {
//            // 异常处理
//            log.error("Error occurred when likeDanmu", e);
//            Thread.currentThread().interrupt(); // 重置中断状态
//            return null;
//        }

    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        // 检查认证信息和分页参数的有效性
        if (auth == null || !isValidAuth(auth) || pageSize <= 0 || pageNum <= 0) {
            return null;
        }

        Long userMid = auth.getMid();
        if (userMid == 0 && (auth.getQq() != null || auth.getWechat() != null)) {
            if (getMidFromAuthInfo(auth) == null) {
                return null;
            }else {
                userMid = getMidFromAuthInfo(auth);
            }
        }

        //过不了oj 可能是at least？？？先标记一下
//        Future<List<Long>> future = executorService.submit(() -> {

        String sql = "SELECT fr2.user_Mid, COUNT(*) as common_followings, u.level " +
                "FROM following_relation fr1 " +
                "JOIN following_relation fr2 ON fr1.follow_Mid = fr2.follow_Mid AND fr1.user_Mid != fr2.user_Mid " +
                "JOIN Users u ON fr2.user_Mid = u.mid " +
                "WHERE fr1.user_Mid = ? AND fr2.user_Mid NOT IN (SELECT follow_Mid FROM following_relation WHERE user_Mid = ?) " +
                "GROUP BY fr2.user_Mid, u.level " +
                "ORDER BY common_followings DESC, u.level DESC, fr2.user_Mid ASC " +
                "LIMIT ? OFFSET ?";
//


        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            List<Long> recommendedUserIds = new ArrayList<>();

            int offset = (pageNum - 1) * pageSize;
            pstmt.setLong(1, userMid);
            pstmt.setLong(2, userMid);
            pstmt.setInt(3, pageSize);
            pstmt.setInt(4, offset);

            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                Long userId = rs.getLong("user_Mid");
                recommendedUserIds.add(userId);
            }
            return recommendedUserIds;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

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






}
