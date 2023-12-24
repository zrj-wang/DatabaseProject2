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
    @PostConstruct
    public void init() {
        executorService = Executors.newFixedThreadPool(10);
    }


    @Override
    public List<String> recommendNextVideo(String bv) {
        // 创建用于存储推荐视频 bv 的列表
        Future<List<String>> future = executorService.submit(() -> {
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
                    "ORDER BY similarity DESC " +
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

        });

        try {

            return future.get();
        } catch (InterruptedException  | ExecutionException e) {
            log.error("Error occurred while recommendNextVideo", e);
            Thread.currentThread().interrupt();
            return null;
        }


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











    //done
    @Override
    public List<String> generalRecommendations(int pageSize, int pageNum) {


        Future<List<String>> future = executorService.submit(() -> {
            // 检查分页参数的有效性
            if (pageSize <= 0 || pageNum <= 0) {
                return null;
            }

            List<String> recommendedVideos = new ArrayList<>();
            String sql = "SELECT v.BV, "
                    + "COALESCE(LEAST(like_rate, 1), 0) AS like_rate, "
                    + "COALESCE(LEAST(coin_rate, 1), 0) AS coin_rate, "
                    + "COALESCE(LEAST(fav_rate, 1), 0) AS fav_rate, "
                    + "COALESCE(danmu_avg, 0) AS danmu_avg, "
                    + "COALESCE(finish_avg, 0) AS finish_avg, "
                    + "(COALESCE(LEAST(like_rate, 1), 0) + COALESCE(LEAST(coin_rate, 1), 0) + COALESCE(LEAST(fav_rate, 1), 0) + COALESCE(danmu_avg, 0) + COALESCE(finish_avg, 0)) AS score "
                    + "FROM videos v "
                    + "LEFT JOIN (SELECT video_like_BV, COUNT(*) / NULLIF((SELECT COUNT(*) FROM watched_relation WHERE video_watched_BV = video_like_BV), 0) AS like_rate FROM liked_relation GROUP BY video_like_BV) lr ON v.BV = lr.video_like_BV "
                    + "LEFT JOIN (SELECT video_coin_BV, COUNT(*) / NULLIF((SELECT COUNT(*) FROM watched_relation WHERE video_watched_BV = video_coin_BV), 0) AS coin_rate FROM coin_relation GROUP BY video_coin_BV) cr ON v.BV = cr.video_coin_BV "
                    + "LEFT JOIN (SELECT video_favorite_BV, COUNT(*) / NULLIF((SELECT COUNT(*) FROM watched_relation WHERE video_watched_BV = video_favorite_BV), 0) AS fav_rate FROM favorite_relation GROUP BY video_favorite_BV) fr ON v.BV = fr.video_favorite_BV "
                    + "LEFT JOIN (SELECT danmu_BV, AVG(danmu_count) AS danmu_avg FROM (SELECT danmu_BV, COUNT(*) AS danmu_count FROM danmu GROUP BY danmu_BV) danmu_grouped GROUP BY danmu_BV) dr ON v.BV = dr.danmu_BV "
                    + "LEFT JOIN (SELECT video_watched_BV, AVG(watched_time / duration) AS finish_avg FROM watched_relation wr JOIN videos vs ON wr.video_watched_BV = vs.BV GROUP BY video_watched_BV) wr ON v.BV = wr.video_watched_BV "
                    + "ORDER BY score DESC "
                    + "LIMIT ? OFFSET ?";

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


        });

        try {
            // 等待异步执行完成并获取结果
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            // 异常处理
            log.error("Error occurred when likeDanmu", e);
            Thread.currentThread().interrupt(); // 重置中断状态
            return null;
        }
    }



    @Override
    public List<String> recommendVideosForUser(AuthInfo auth, int pageSize, int pageNum) {

        Future<List<String>> future = executorService.submit(() -> {
            // 检查认证信息、分页参数的有效性
            if (auth == null || !isValidAuth(auth) || pageSize <= 0 || pageNum <= 0) {
                return null;
            }

            List<String> recommendedVideos = new ArrayList<>();
            String sql = "SELECT v.BV " +
                    "FROM videos v " +
                    "JOIN (SELECT video_watched_BV FROM watched_relation WHERE user_watched_Mid IN ( " +
                    "SELECT user_Mid FROM following_relation fr1 " +
                    "JOIN following_relation fr2 ON fr1.user_Mid = fr2.follow_Mid AND fr1.follow_Mid = fr2.user_Mid " +
                    "WHERE fr1.user_Mid = ?) " +
                    "AND video_watched_BV NOT IN (SELECT video_watched_BV FROM watched_relation WHERE user_watched_Mid = ?) " +
                    "GROUP BY video_watched_BV) watched_videos ON v.BV = watched_videos.video_watched_BV " +
                    "JOIN Users u ON v.owner_Mid = u.mid " +
                    "ORDER BY COUNT(watched_videos.video_watched_BV) DESC, u.level DESC, v.public_time DESC " +
                    "LIMIT ? OFFSET ?";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                int offset = (pageNum - 1) * pageSize;
                pstmt.setLong(1, auth.getMid());
                pstmt.setLong(2, auth.getMid());
                pstmt.setInt(3, pageSize);
                pstmt.setInt(4, offset);

                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    String bv = rs.getString("BV");
                    recommendedVideos.add(bv);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }

            // 如果用户兴趣为空，则返回 generalRecommendations 的结果
            if (recommendedVideos.isEmpty()) {
                return generalRecommendations(pageSize, pageNum);
            }

            return recommendedVideos;


        });

        try {
            // 等待异步执行完成并获取结果
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            // 异常处理
            log.error("Error occurred when likeDanmu", e);
            Thread.currentThread().interrupt(); // 重置中断状态
            return null;
        }
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

                // 使用 SHA-256 对用户输入的密码进行加密
                String encryptedInputPassword = hashPasswordWithSHA256(auth.getPassword());

                // 检查加密后的密码是否匹配
                boolean isPasswordValid = encryptedInputPassword.equals(storedPassword);
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





    @Override
    public List<Long> recommendFriends(AuthInfo auth, int pageSize, int pageNum) {
        // 检查认证信息和分页参数的有效性
        if (auth == null || !isValidAuth(auth) || pageSize <= 0 || pageNum <= 0) {
            return null;
        }
        Future<List<Long>> future = executorService.submit(() -> {
            List<Long> recommendedUserIds = new ArrayList<>();
            String sql = "SELECT fr2.user_Mid, COUNT(*) as common_followings, u.level " +
                    "FROM following_relation fr1 " +
                    "JOIN following_relation fr2 ON fr1.follow_Mid = fr2.follow_Mid AND fr1.user_Mid != fr2.user_Mid " +
                    "JOIN Users u ON fr2.user_Mid = u.mid " +
                    "WHERE fr1.user_Mid = ? AND fr2.user_Mid NOT IN (SELECT follow_Mid FROM following_relation WHERE user_Mid = ?) " +
                    "GROUP BY fr2.user_Mid, u.level " +
                    "ORDER BY common_followings DESC, u.level, fr2.user_Mid ASC " +
                    "LIMIT ? OFFSET ?";

            try (Connection conn = dataSource.getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                int offset = (pageNum - 1) * pageSize;
                pstmt.setLong(1, auth.getMid());
                pstmt.setLong(2, auth.getMid());
                pstmt.setInt(3, pageSize);
                pstmt.setInt(4, offset);

                ResultSet rs = pstmt.executeQuery();
                while (rs.next()) {
                    Long userId = rs.getLong("user_Mid");
                    recommendedUserIds.add(userId);
                }
            } catch (SQLException e) {
                e.printStackTrace();
                return null;
            }

            return recommendedUserIds.isEmpty() ? new ArrayList<>() : recommendedUserIds;


        });

        try {
            // 等待异步执行完成并获取结果
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            // 异常处理
            log.error("Error occurred when likeDanmu", e);
            Thread.currentThread().interrupt(); // 重置中断状态
            return null;
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

}
