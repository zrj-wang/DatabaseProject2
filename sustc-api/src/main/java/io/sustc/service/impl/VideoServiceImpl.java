package io.sustc.service.impl;
import io.sustc.dto.AuthInfo;
import io.sustc.dto.PostVideoReq;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.sustc.dto.VideoRecord;
import io.sustc.service.UserService;
import io.sustc.service.VideoService;
import org.springframework.stereotype.Service;


//jz-gong
@Service
@Slf4j
public class VideoServiceImpl implements VideoService{
    @Autowired
    private DataSource dataSource;

    private ExecutorService executorService;
    @PostConstruct
    public void init() {
        executorService = Executors.newFixedThreadPool(10);
    }

    public String postVideo(AuthInfo auth, PostVideoReq req) {
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return null;
        }

        Long mid;
        if(auth.getMid() != 0){
            mid = auth.getMid();
        } else {
            // 验证 auth 是否有效，并从中获取用户的 mid
            mid = getMidFromAuthInfo(auth);
            if (mid == null) {
                return null; // 用户认证无效或无法获取用户 ID
            }
        }

        // 验证 req 是否有效
        if (req == null || req.getTitle() == null || req.getTitle().isEmpty() || req.getPublicTime()==null||
                req.getDuration() < 10 ||
                ( req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now())) ||
                isTitleExist(mid, req.getTitle())) {
            return null;
        }

        String bv;
        do {
            bv = generateBV();
        } while (isBVExist(bv)); // 重复生成BV直到找到一个独一无二的值

        // 实现视频发布的逻辑
        if (insertVideoInfo(mid, bv, req)) {
            return bv;
        } else {
            return null;
        }
    }


    // 将视频信息插入数据库
    private boolean insertVideoInfo(long mid, String bv, PostVideoReq req) {
        String sql = "INSERT INTO videos (BV, title, owner_Mid, commit_time, review_time, public_time, duration, description) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, bv);
            pstmt.setString(2, req.getTitle());
            pstmt.setLong(3, mid);
            pstmt.setTimestamp(4, Timestamp.valueOf(LocalDateTime.now())); // commit_time
            pstmt.setTimestamp(5, null); // review_time
            pstmt.setTimestamp(6, req.getPublicTime()); // public_time
            pstmt.setLong(7, (long) req.getDuration());
            pstmt.setString(8, req.getDescription());

            int rowsAffected = pstmt.executeUpdate();
            if (rowsAffected > 0) {
                return true;
            } else {
                return false;
            }
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

    private boolean isTitleExist(long mid, String title) {
        String sql = "SELECT COUNT(*) FROM videos WHERE owner_Mid = ? AND title = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, mid);
            pstmt.setString(2, title);

            ResultSet rs = pstmt.executeQuery();
            if (rs.next() && rs.getInt(1) > 0) {
                return true; // 如果找到同名视频，返回 true
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // 如果没有找到同名视频或发生异常，返回 false
    }

    private String generateBV() {
        String charSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder("BV");
        Random random = new Random();

        // 生成10个随机字符
        for (int i = 0; i < 10; i++) {
            int index = random.nextInt(charSet.length());
            sb.append(charSet.charAt(index));
        }

        return sb.toString();
    }

    private boolean isBVExist(String bv) {
        String sql = "SELECT COUNT(*) FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, bv);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next() && rs.getInt(1) > 0) {
                return true; // 如果找到相同的BV，返回 true
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false; // 如果没有找到相同的BV或发生异常，返回 false
    }


    public boolean deleteVideo(AuthInfo auth, String bv) {

            // 验证 auth 是否有效
            if (!isValidAuth(auth)) {
                return false;
            }

            Long mid;
            if(auth.getMid() != 0){
                mid = auth.getMid();
            } else {
                // 验证 auth 是否有效，并从中获取用户的 mid
                mid = getMidFromAuthInfo(auth);
                if (mid == null) {
                    return false; // 用户认证无效或无法获取用户 ID
                }
            }

            // 检查 bv 是否无效
            if (bv == null || bv.isEmpty() ) {
                return false;
            }



            // 执行删除视频的操作
            return performDeleteVideo(mid,bv);



    }

    private boolean videoExists(String bv) {
        // SQL 查询语句
        String sql = "SELECT COUNT(*) FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, bv);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 检查查询结果
            if (rs.next() && rs.getInt(1) > 0) {
                // 如果计数大于0，视频存在
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 如果没有找到视频或发生异常，返回 false
        return false;
    }






    public boolean performDeleteVideo(Long mid, String bv) {
        try (Connection conn = dataSource.getConnection()) {
            // 检查视频是否存在
            String checkVideoSql = "SELECT owner_Mid FROM videos WHERE BV = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(checkVideoSql)) {
                pstmt.setString(1, bv);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (!rs.next()) {
                        return false; // 视频不存在
                    }
                    Long ownerMid = rs.getLong("owner_Mid");
                    if (!mid.equals(ownerMid)) {
                        // 检查用户是否是超级用户或视频的所有者
                        String checkUserSql = "SELECT identity FROM users WHERE mid = ?";
                        try (PreparedStatement userPstmt = conn.prepareStatement(checkUserSql)) {
                            userPstmt.setLong(1, mid);
                            try (ResultSet userRs = userPstmt.executeQuery()) {
                                if (!userRs.next() || !"SUPERUSER".equals(userRs.getString("identity"))) {
                                    return false; // 用户不是超级用户也不是视频的所有者
                                }
                            }
                        }
                    }
                }
            }

            // 删除videos表中的数据
            String deleteVideoSql = "DELETE FROM videos WHERE BV = ?";
            try (PreparedStatement deletePstmt = conn.prepareStatement(deleteVideoSql)) {
                deletePstmt.setString(1, bv);
                deletePstmt.executeUpdate();
            }

            // 删除 danmu 表中与视频相关的弹幕
            String deleteDanmuSql = "DELETE FROM danmu WHERE danmu_BV = ?";
            try (PreparedStatement deletePstmt = conn.prepareStatement(deleteDanmuSql)) {
                deletePstmt.setString(1, bv);
                deletePstmt.executeUpdate();
            }

            // 删除 danmuliked_relation 表中与视频相关的弹幕喜欢关系
            String deleteDanmuLikedSql = "DELETE FROM danmuliked_relation WHERE danmu_liked_id IN (SELECT danmu_id FROM danmu WHERE danmu_BV = ?)";
            try (PreparedStatement deletePstmt = conn.prepareStatement(deleteDanmuLikedSql)) {
                deletePstmt.setString(1, bv);
                deletePstmt.executeUpdate();
            }

            // 删除 liked_relation 表中与视频相关的点赞关系
            String deleteLikedSql = "DELETE FROM liked_relation WHERE video_like_BV = ?";
            try (PreparedStatement deletePstmt = conn.prepareStatement(deleteLikedSql)) {
                deletePstmt.setString(1, bv);
                deletePstmt.executeUpdate();
            }

            // 删除 coin_relation 表中与视频相关的投币关系
            String deleteCoinSql = "DELETE FROM coin_relation WHERE video_coin_BV = ?";
            try (PreparedStatement deletePstmt = conn.prepareStatement(deleteCoinSql)) {
                deletePstmt.setString(1, bv);
                deletePstmt.executeUpdate();
            }

            // 删除 watched_relation 表中与视频相关的观看记录
            String deleteWatchedSql = "DELETE FROM watched_relation WHERE video_watched_BV = ?";
            try (PreparedStatement deletePstmt = conn.prepareStatement(deleteWatchedSql)) {
                deletePstmt.setString(1, bv);
                deletePstmt.executeUpdate();
            }

            // 删除 favorite_relation 表中与视频相关的收藏关系
            String deleteFavoriteSql = "DELETE FROM favorite_relation WHERE video_favorite_BV = ?";
            try (PreparedStatement deletePstmt = conn.prepareStatement(deleteFavoriteSql)) {
                deletePstmt.setString(1, bv);
                deletePstmt.executeUpdate();
            }

            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }





    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        // 验证 auth 是否有效
        if(!isValidAuth(auth)){
            return false;
        }
        Long userId;
        if(auth.getMid()!=0){
            userId=auth.getMid();
        }else{
            // 验证 auth 是否有效，并从中获取用户的 mid
            userId=getMidFromAuthInfo(auth);
            if (userId == null) {

                return false; // 用户认证无效或无法获取用户 ID
            }
        }

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty()  ) {
            return false;
        }

        // 验证 req 是否有效
        if (req == null || req.getTitle() == null || req.getTitle().isEmpty() || req.getPublicTime()==null||
                req.getDuration() < 10 ||
                ( req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now())) ||
                isTitleExist(userId, req.getTitle())) {
            return false;
        }



        // 执行更新视频信息的操作
        return performUpdateVideoInfo(userId,bv, req);
    }

    private boolean performUpdateVideoInfo(long mid, String bv, PostVideoReq req) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        PreparedStatement pstmtCheckOwnerAndReview = null;
        PreparedStatement pstmtCheckCurrentInfo = null;
        ResultSet rs = null;
        ResultSet rsCurrentInfo = null;
        boolean wasReviewedBefore = false;


        try {
            conn = dataSource.getConnection();
//            conn.setAutoCommit(false); // 开始事务，禁用自动提交

            // 检查视频是否存在，验证视频拥有者，以及视频是否已被审查过
            String sqlCheckOwnerAndReview = "SELECT owner_mid, duration, review_time FROM videos WHERE BV = ?";
            pstmtCheckOwnerAndReview = conn.prepareStatement(sqlCheckOwnerAndReview);
            pstmtCheckOwnerAndReview.setString(1, bv);
            rs = pstmtCheckOwnerAndReview.executeQuery();

            if (!rs.next() || rs.getLong("owner_mid") != mid) {
//                conn.rollback(); // 视频不存在或用户不是视频拥有者
                return false;
            }

            // 检查视频时长是否与传入的时长匹配
            if (req.getDuration() != rs.getFloat("duration")) {
//                conn.rollback(); // 时长不匹配
                return false;
            }

            // 检查视频是否已经被审查过
            wasReviewedBefore = rs.getTimestamp("review_time") != null;

            // 检查视频当前信息是否与传入的信息相同
            String sqlCheckCurrentInfo = "SELECT title, public_time, description FROM videos WHERE BV = ?";
            pstmtCheckCurrentInfo = conn.prepareStatement(sqlCheckCurrentInfo);
            pstmtCheckCurrentInfo.setString(1, bv);
            rsCurrentInfo = pstmtCheckCurrentInfo.executeQuery();

            if (rsCurrentInfo.next()) {
                String currentTitle = rsCurrentInfo.getString("title");
                Timestamp currentPublicTime = rsCurrentInfo.getTimestamp("public_time");
                String currentDescription = rsCurrentInfo.getString("description");

                if (currentTitle.equals(req.getTitle()) &&
                        currentPublicTime.equals(req.getPublicTime()) &&
                        currentDescription.equals(req.getDescription())) {
//                    conn.rollback(); // 信息没有变化
                    return false;
                }
            }

            // 更新视频信息
            String sqlUpdate = "UPDATE videos SET title = ?, public_time = ?, description = ?, review_time = NULL, reviewer = NULL WHERE BV = ?";
            pstmt = conn.prepareStatement(sqlUpdate);
            pstmt.setString(1, req.getTitle());
            pstmt.setTimestamp(2, req.getPublicTime());
            pstmt.setString(3, req.getDescription());
            pstmt.setString(4, bv);

            int rowsAffected = pstmt.executeUpdate();
            if (rowsAffected > 0) {
//                conn.commit(); // 成功执行更新，提交事务
                return wasReviewedBefore;
            } else {
//                conn.rollback(); // 更新未执行，回滚事务
                return false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
//            if (conn != null) {
//                try {
//                    conn.rollback(); // 发生异常时回滚事务
//                } catch (SQLException ex) {
//                    ex.printStackTrace();
//                }
//            }
            return false;
        }
//        finally {
//            // 关闭资源
//            try {
//                if (rsCurrentInfo != null) rsCurrentInfo.close();
//                if (rs != null) rs.close();
//                if (pstmtCheckCurrentInfo != null) pstmtCheckCurrentInfo.close();
//                if (pstmtCheckOwnerAndReview != null) pstmtCheckOwnerAndReview.close();
//                if (pstmt != null) pstmt.close();
//                if (conn != null) conn.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
    }








    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        Future<List<String>> future = executorService.submit(() -> {
            if(!isValidAuth(auth)){
                return null;
            }
            Long userId;
            if(auth.getMid()!=0){
                userId=auth.getMid();
            }else{
            // 验证 auth 是否有效，并从中获取用户的 mid
            userId=getMidFromAuthInfo(auth);
            if (userId == null) {

                return null; // 用户认证无效或无法获取用户 ID
                }
            }

            // 检查 keywords 是否无效
            if (keywords == null || keywords.isEmpty()) {
                return null;
            }

            // 检查 pageSize 和 pageNum 是否无效
            if (pageSize <= 0 || pageNum <= 0) {
                return null;
            }

            // 执行搜索视频的操作
            return performSearchVideo(keywords, pageSize, pageNum, userId);
        });

        try {
            // 等待异步执行完成并获取结果
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            // 异常处理
            log.error("Error occurred when searchVideo", e);
            Thread.currentThread().interrupt(); // 重置中断状态
            return null;
        }
    }


    private List<String> performSearchVideo(String keywords, int pageSize, int pageNum, long userId) {
        // 分割关键词
        String[] keywordArray = keywords.split("\\s+");

        // 构建基础查询
        String baseQuery = "WITH split_keywords AS (" +
                "SELECT unnest(string_to_array(LOWER(?), ' ')) AS keyword), " +
                "views AS (" +
                "SELECT video_watched_BV AS BV, COUNT(*) AS view_count " +
                "FROM watched_relation " +
                "GROUP BY video_watched_BV), " +
                "keyword_search AS (" +
                "SELECT v.BV, v.title, v.description, u.name AS owner_name, " +
                "COALESCE(vws.view_count, 0) AS view_count, " +
                "SUM(CASE " +
                "WHEN LENGTH(LOWER(sk.keyword)) > 0 THEN " +
                "(LENGTH(LOWER(v.title)) - LENGTH(REPLACE(LOWER(v.title), LOWER(sk.keyword), ''))) / NULLIF(LENGTH(LOWER(sk.keyword)), 0) + " +
                "(LENGTH(LOWER(v.description)) - LENGTH(REPLACE(LOWER(v.description), LOWER(sk.keyword), ''))) / NULLIF(LENGTH(LOWER(sk.keyword)), 0) + " +
                "(LENGTH(LOWER(u.name)) - LENGTH(REPLACE(LOWER(u.name), LOWER(sk.keyword), ''))) / NULLIF(LENGTH(LOWER(sk.keyword)), 0) " +
                "ELSE 0 " +
                "END) AS relevance " +
                "FROM videos v " +
                "JOIN users u ON v.owner_Mid = u.mid " +
                "LEFT JOIN views vws ON v.BV = vws.BV, " +
                "split_keywords sk " +
                "WHERE " +
                "(LOWER(v.title) LIKE '%' || LOWER(sk.keyword) || '%'" +
                "OR LOWER(v.description) LIKE '%' || LOWER(sk.keyword) || '%' "+
                "OR LOWER(u.name) LIKE '%' || LOWER(sk.keyword) || '%') " +
                "AND" +
                "((v.reviewer != 0 AND v.review_time IS NOT NULL AND v.public_time <= CURRENT_TIMESTAMP) " +
                "OR u.mid = ? OR u.identity ='SUPERUSER') " +
                "GROUP BY v.BV, u.name, v.title, v.description, vws.view_count) " +
                "SELECT k.BV, k.title, k.owner_name, k.description, k.relevance, k.view_count " +
                "FROM keyword_search k " +
                "WHERE k.relevance IS NOT NULL AND k.relevance > 0 " +
                "ORDER BY k.relevance DESC, k.view_count DESC " +
                "LIMIT ? OFFSET ?;";



        // 执行查询并处理结果
        List<String> bvList = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(baseQuery)) {

            pstmt.setString(1, keywords); // 第一个参数，关键字字符串
            pstmt.setLong(2, userId); // 第二个参数，用户的 mid
            pstmt.setInt(3, pageSize); // 第三个参数，页面大小
            pstmt.setInt(4, (pageNum - 1) * pageSize); // 第四个参数，计算出的偏移量

            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                bvList.add(rs.getString("BV"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return bvList;
    }







    public double getAverageViewRate(String bv) {
        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty() || !videoExists(bv)) {
            return -1;
        }

        // 执行计算平均观看率的操作
        return performGetAverageViewRate(bv);
    }

    private double performGetAverageViewRate(String bv) {
        // SQL 查询语句
        String sql = "SELECT AVG(CAST(w.watched_time AS FLOAT) / v.duration) AS average_view_rate " +
                "FROM watched_relation w INNER JOIN videos v ON w.video_watched_BV = v.BV " +
                "WHERE w.video_watched_BV = ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, bv);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 检查查询结果
            if (rs.next()) {
                double avgRate = rs.getDouble("average_view_rate");
                if (rs.wasNull()) {
                    // 如果结果是空的（即没有人观看视频），返回 -1
                    return -1;
                }
                // 返回计算出的平均观看率
                return avgRate;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        // 如果没有找到视频或发生异常，返回 -1
        return -1;
    }


    /**
     * Gets the hotspot of a video.
     * With splitting the video into 10-second chunks, hotspots are defined as chunks with the most danmus.
     *
     * @param bv the video's {@code bv}
     * @return the index of hotspot chunks (start from 0)
     * @apiNote You may consider the following corner cases:
     * <ul>
     * <<<<<<< HEAD
     *   <li>{@code bv} is invalid (null or empty or not found)</li>
     * =======
     *   <li>cannot find a video corresponding to the {@code bv}</li>
     * >>>>>>> upstream/main
     *   <li>no one has sent danmu on this video</li>
     * </ul>
     * If any of the corner case happened, an empty set shall be returned.
     */
    public Set<Integer> getHotspot(String bv) {
        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty() || !videoExists(bv)) {
            return Collections.emptySet();
        }

        // 执行计算热点的操作
        return performGetHotspot(bv);
    }

    private Set<Integer> performGetHotspot(String bv) {
        // 修改后的 SQL 查询语句
        String sql = "SELECT chunk, cnt " +
                "FROM (SELECT FLOOR(time/10) AS chunk, COUNT(*) AS cnt " +
                "FROM danmu " +
                "WHERE danmu_BV = ? " +
                "GROUP BY chunk) AS subquery " +
                "WHERE cnt = (SELECT MAX(cnt) " +
                "FROM (SELECT COUNT(*) AS cnt " +
                "FROM danmu " +
                "WHERE danmu_BV = ? " +
                "GROUP BY FLOOR(time/10)) AS dc)";

        Set<Integer> hotspots = new HashSet<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, bv);
            pstmt.setString(2, bv); // 为内部查询设置相同的参数

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 处理查询结果
            if (!rs.isBeforeFirst()) {
                // 如果结果集为空，表示没有弹幕，返回空集合
                return Collections.emptySet();
            }

            while (rs.next()) {
                // 将每个热点时间段的索引添加到集合中
                hotspots.add(rs.getInt("chunk"));
            }
            return hotspots;
        } catch (SQLException e) {
            // 异常处理逻辑
            e.printStackTrace(); // 记录异常信息，有助于调试
        }
        // 如果发生异常，返回空集合
        return Collections.emptySet();
    }


    public boolean reviewVideo(AuthInfo auth, String bv) {
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        Long mid;
        if(auth.getMid() != 0){
            mid = auth.getMid();
        } else {
            // 验证 auth 是否有效，并从中获取用户的 mid
            mid = getMidFromAuthInfo(auth);
            if (mid == null) {
                return false; // 用户认证无效或无法获取用户 ID
            }
        }

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty() ) {
            return false;
        }







        // 执行审核视频的操作
        return performReviewVideo(mid,bv);
    }

    private boolean performReviewVideo(long mid, String bv) {
        Connection conn = null;
        PreparedStatement pstmtCheckUser = null;
        PreparedStatement pstmtCheckVideo = null;
        PreparedStatement pstmtUpdateReview = null;
        ResultSet rsUser = null;
        ResultSet rsVideo = null;

        try {
            conn = dataSource.getConnection();
//            conn.setAutoCommit(false); // 开始事务

            // 检查用户是否为 SUPERUSER
            String sqlCheckUser = "SELECT identity FROM users WHERE mid = ?";
            pstmtCheckUser = conn.prepareStatement(sqlCheckUser);
            pstmtCheckUser.setLong(1, mid);
            rsUser = pstmtCheckUser.executeQuery();
            if (!rsUser.next() || !"SUPERUSER".equals(rsUser.getString("identity"))) {
//                conn.rollback();
                return false;
            }

            // 检查视频是否存在且未被审查
            String sqlCheckVideo = "SELECT review_time, reviewer FROM videos WHERE BV = ?";
            pstmtCheckVideo = conn.prepareStatement(sqlCheckVideo);
            pstmtCheckVideo.setString(1, bv);
            rsVideo = pstmtCheckVideo.executeQuery();
            if (!rsVideo.next() || rsVideo.getTimestamp("review_time") != null || rsVideo.getLong("reviewer") != 0) {
//                conn.rollback();
                return false;
            }

            // 更新视频审查信息
            String sqlUpdateReview = "UPDATE videos SET review_time = ?, reviewer = ? WHERE BV = ?";
            pstmtUpdateReview = conn.prepareStatement(sqlUpdateReview);
            pstmtUpdateReview.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
            pstmtUpdateReview.setLong(2, mid);
            pstmtUpdateReview.setString(3, bv);
            pstmtUpdateReview.executeUpdate();

//            conn.commit(); // 提交事务
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
//            if (conn != null) {
//                try {
//                    conn.rollback(); // 发生异常时回滚事务
//                } catch (SQLException ex) {
//                    ex.printStackTrace();
//                }
//            }
            return false;
        }
//        finally {
//            // 关闭资源
//            try {
//                if (rsUser != null) rsUser.close();
//                if (rsVideo != null) rsVideo.close();
//                if (pstmtCheckUser != null) pstmtCheckUser.close();
//                if (pstmtCheckVideo != null) pstmtCheckVideo.close();
//                if (pstmtUpdateReview != null) pstmtUpdateReview.close();
//                if (conn != null) conn.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
    }

    private boolean isVideoReviewed(String bv) {
        // SQL 查询语句
        String sql = "SELECT review_time FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, bv);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 检查查询结果
            if (rs.next()) {
                // 获取 review_time 字段的值
                Timestamp reviewTime = rs.getTimestamp("review_time");
                // 如果 review_time 不为 null，则视频已被审核
                return reviewTime != null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息
        }
        // 如果查询不到视频或发生异常，假设视频未被审核
        return false;
    }

    public boolean coinVideo(AuthInfo auth, String bv) {
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        Long mid;
        if(auth.getMid() != 0){
            mid = auth.getMid();
        } else {
            // 验证 auth 是否有效，并从中获取用户的 mid
            mid = getMidFromAuthInfo(auth);
            if (mid == null) {
                return false; // 用户认证无效或无法获取用户 ID
            }
        }

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty()) {
            return false;
        }

        // 检查用户是否能搜索到这个视频
        if (!canUserSearchAndCoinVideo(mid, bv)) {
            return false;
        }


        // 执行投币操作
        return performCoinVideo(mid, bv);
    }


    // 检查用户是否能够搜索到视频
    // 检查用户是否能够搜索到视频、是否已经给这个视频投过币，以及是否还有剩余的硬币
    private boolean canUserSearchAndCoinVideo(Long mid, String bv) {
        // SQL 查询1：检查用户是否能搜索到视频
        String sqlSearch = "SELECT COUNT(*) FROM videos v JOIN users u ON v.owner_Mid = u.mid " +
                "WHERE v.BV = ? AND " +
                "((v.reviewer != 0 AND v.review_time IS NOT NULL AND v.public_time <= CURRENT_TIMESTAMP) OR u.identity = 'SUPERUSER') " +
                "AND v.owner_Mid != ?";
        // SQL 查询2：检查用户是否已经给这个视频投过币
        String sqlCoin = "SELECT COUNT(*) FROM coin_relation WHERE video_coin_BV = ? AND user_coin_Mid = ?";
        // SQL 查询3：检查用户是否还有剩余的硬币
        String sqlCoinsLeft = "SELECT coin FROM users WHERE mid = ?";

        try (Connection conn = dataSource.getConnection();  // 建立数据库连接
             PreparedStatement pstmtSearch = conn.prepareStatement(sqlSearch);  // 创建 PreparedStatement 用于第一个查询
             PreparedStatement pstmtCoin = conn.prepareStatement(sqlCoin);  // 创建 PreparedStatement 用于第二个查询
             PreparedStatement pstmtCoinsLeft = conn.prepareStatement(sqlCoinsLeft)) {  // 创建 PreparedStatement 用于第三个查询

            // 执行第一个查询：检查用户是否能搜索到视频
            pstmtSearch.setString(1, bv); // 设置第一个参数，视频的 BV
            pstmtSearch.setLong(2, mid);  // 设置第二个参数，用户的 mid
            ResultSet rsSearch = pstmtSearch.executeQuery();

            boolean canSearch = false;
            if (rsSearch.next()) {
                canSearch = rsSearch.getInt(1) > 0;
            }

            // 如果用户可以搜索到视频，继续执行后续查询
            if (canSearch) {
                // 执行第二个查询：检查用户是否已经给这个视频投过币
                pstmtCoin.setString(1, bv); // 设置第一个参数，视频的 BV
                pstmtCoin.setLong(2, mid);  // 设置第二个参数，用户的 mid
                ResultSet rsCoin = pstmtCoin.executeQuery();

                boolean notDonatedCoin = false;
                if (rsCoin.next()) {
                    notDonatedCoin = rsCoin.getInt(1) == 0;
                }

                // 如果用户还没有给这个视频投过币，检查用户是否还有剩余的硬币
                if (notDonatedCoin) {
                    pstmtCoinsLeft.setLong(1, mid);  // 设置参数，用户的 mid
                    ResultSet rsCoinsLeft = pstmtCoinsLeft.executeQuery();

                    if (rsCoinsLeft.next()) {
                        return rsCoinsLeft.getInt(1) > 0; // 如果用户的硬币数大于0，返回 true
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false; // 发生 SQL 异常时返回 false
        }

        return false; // 默认返回 false
    }



    public boolean performCoinVideo(Long mid, String bv) {
        // SQL 查询1：减少用户的硬币数量
        String sqlDecreaseCoin = "UPDATE users SET coin = coin - 1 WHERE mid = ? AND coin > 0";
        // SQL 查询2：在 coin_relation 表中插入新记录
        String sqlInsertCoinRelation = "INSERT INTO coin_relation (video_coin_BV, user_coin_Mid) VALUES (?, ?)";



        try (Connection conn=dataSource.getConnection();
             PreparedStatement pstmtDecreaseCoin = conn.prepareStatement(sqlDecreaseCoin);
             PreparedStatement pstmtInsertCoinRelation = conn.prepareStatement(sqlInsertCoinRelation)){
            // 获取连接并开启事务
//            conn.setAutoCommit(false); // 关闭自动提交

            // 执行第一个操作：减少用户的硬币数量

            pstmtDecreaseCoin.setLong(1, mid);
            int updatedRows = pstmtDecreaseCoin.executeUpdate();

            // 检查是否有硬币减少，如果没有，则回滚并返回 false
            if (updatedRows == 0) {
//                conn.rollback();
                return false;
            }

            // 执行第二个操作：在 coin_relation 表中插入新记录
            pstmtInsertCoinRelation.setString(1, bv);
            pstmtInsertCoinRelation.setLong(2, mid);
            pstmtInsertCoinRelation.executeUpdate();

            // 提交事务
//            conn.commit();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
//            if (conn != null) {
//                try {
//                    conn.rollback(); // 发生异常时回滚事务
//                } catch (SQLException ex) {
//                    ex.printStackTrace();
//                }
//            }
            return false;
        }
//        finally {
//            // 关闭资源
//            try {
//                if (pstmtDecreaseCoin != null) pstmtDecreaseCoin.close();
//                if (pstmtInsertCoinRelation != null) pstmtInsertCoinRelation.close();
//                if (conn != null) conn.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
    }




    public boolean likeVideo(AuthInfo auth, String bv) {
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        Long mid;
        if(auth.getMid() != 0){
            mid = auth.getMid();
        } else {
            // 验证 auth 是否有效，并从中获取用户的 mid
            mid = getMidFromAuthInfo(auth);
            if (mid == null) {
                return false; // 用户认证无效或无法获取用户 ID
            }
        }

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty() ) {
            return false;
        }

        // 检查用户是否能搜索到这个视频
        if (!canUserSearchAndLikeVideo(mid, bv)) {
            return false;
        }

        // 执行点赞视频的操作
        return performLikeVideo(mid, bv);
    }


    private boolean canUserSearchAndLikeVideo(Long mid, String bv) {
        // SQL 查询：检查用户是否能搜索到视频
        String sqlSearch = "SELECT COUNT(*) FROM videos v JOIN users u ON v.owner_Mid = u.mid " +
                "WHERE v.BV = ? AND " +
                "((v.reviewer != 0 AND v.review_time IS NOT NULL AND v.public_time <= CURRENT_TIMESTAMP) OR u.identity = 'SUPERUSER') " +
                "AND v.owner_Mid != ?";

        try (Connection conn = dataSource.getConnection();  // 建立数据库连接
             PreparedStatement pstmtSearch = conn.prepareStatement(sqlSearch)) {  // 创建 PreparedStatement

            pstmtSearch.setString(1, bv); // 设置第一个参数，视频的 BV
            pstmtSearch.setLong(2, mid);  // 设置第二个参数，用户的 mid
            ResultSet rsSearch = pstmtSearch.executeQuery();

            if (rsSearch.next()) {
                return rsSearch.getInt(1) > 0; // 如果结果大于0，则用户可以搜索到视频
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false; // 发生 SQL 异常时返回 false
        }

        return false; // 默认返回 false
    }



    public boolean performLikeVideo(Long mid, String bv) {
        // SQL 查询：检查用户是否已经点赞过这个视频
        String sqlCheckLike = "SELECT COUNT(*) FROM liked_relation WHERE video_like_BV = ? AND user_like_Mid = ?";
        // SQL 查询：在 liked_relation 表中插入新记录
        String sqlInsertLikeRelation = "INSERT INTO liked_relation (video_like_BV, user_like_Mid) VALUES (?, ?)";
        // SQL 查询：在 liked_relation 表中删除记录
        String sqlDeleteLikeRelation = "DELETE FROM liked_relation WHERE video_like_BV = ? AND user_like_Mid = ?";

        Connection conn = null;
        PreparedStatement pstmtCheckLike = null;
        PreparedStatement pstmtUpdateLikeRelation = null;

        try {
            // 获取连接并开启事务
            conn = dataSource.getConnection();
//            conn.setAutoCommit(false); // 关闭自动提交

            // 首先检查用户是否已经点赞过视频
            pstmtCheckLike = conn.prepareStatement(sqlCheckLike);
            pstmtCheckLike.setString(1, bv);
            pstmtCheckLike.setLong(2, mid);
            ResultSet rsCheckLike = pstmtCheckLike.executeQuery();

            boolean hasLiked = false;
            if (rsCheckLike.next()) {
                hasLiked = rsCheckLike.getInt(1) > 0;
            }

            // 根据点赞状态执行相应的操作
            if (hasLiked) {
                // 用户已点赞，执行取消点赞操作
                pstmtUpdateLikeRelation = conn.prepareStatement(sqlDeleteLikeRelation);
            } else {
                // 用户未点赞，执行点赞操作
                pstmtUpdateLikeRelation = conn.prepareStatement(sqlInsertLikeRelation);
            }

            pstmtUpdateLikeRelation.setString(1, bv);
            pstmtUpdateLikeRelation.setLong(2, mid);
            pstmtUpdateLikeRelation.executeUpdate();

            // 提交事务
//            conn.commit();
            return !hasLiked;
        } catch (SQLException e) {
            e.printStackTrace();
//            if (conn != null) {
//                try {
//                    conn.rollback(); // 发生异常时回滚事务
//                } catch (SQLException ex) {
//                    ex.printStackTrace();
//                }
//            }
            return false;
        }

//        finally {
//            // 关闭资源
//            try {
//                if (pstmtCheckLike != null) pstmtCheckLike.close();
//                if (pstmtUpdateLikeRelation != null) pstmtUpdateLikeRelation.close();
//                if (conn != null) conn.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
    }


    public boolean collectVideo(AuthInfo auth, String bv){
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        Long mid;
        if(auth.getMid() != 0){
            mid = auth.getMid();
        } else {
            // 验证 auth 是否有效，并从中获取用户的 mid
            mid = getMidFromAuthInfo(auth);
            if (mid == null) {
                return false; // 用户认证无效或无法获取用户 ID
            }
        }

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty()) {
            return false;
        }

        // 检查用户是否能搜索到这个视频
        if (!canUserSearchAndCollectVideo(mid, bv)) {
            return false;
        }

        // 执行收藏视频的操作
        return performCollectVideo(mid, bv);
    }




    private boolean canUserSearchAndCollectVideo(Long mid, String bv) {
        // SQL 查询：检查用户是否能搜索到视频
        String sqlSearch = "SELECT COUNT(*) FROM videos v JOIN users u ON v.owner_Mid = u.mid " +
                "WHERE v.BV = ? AND " +
                "((v.reviewer != 0 AND v.review_time IS NOT NULL AND v.public_time <= CURRENT_TIMESTAMP) OR u.identity = 'SUPERUSER') " +
                "AND v.owner_Mid != ?";

        try (Connection conn = dataSource.getConnection();  // 建立数据库连接
             PreparedStatement pstmtSearch = conn.prepareStatement(sqlSearch)) {  // 创建 PreparedStatement

            pstmtSearch.setString(1, bv); // 设置第一个参数，视频的 BV
            pstmtSearch.setLong(2, mid);  // 设置第二个参数，用户的 mid
            ResultSet rsSearch = pstmtSearch.executeQuery();

            if (rsSearch.next()) {
                return rsSearch.getInt(1) > 0; // 如果结果大于0，则用户可以搜索到视频
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false; // 发生 SQL 异常时返回 false
        }

        return false; // 默认返回 false
    }




    public boolean performCollectVideo(Long mid, String bv) {
        // SQL 查询：检查用户是否已经收藏过这个视频
        String sqlCheckCollect = "SELECT COUNT(*) FROM favorite_relation WHERE video_favorite_BV = ? AND user_favorite_Mid = ?";
        // SQL 查询：在 favorite_relation 表中插入新记录
        String sqlInsertCollectRelation = "INSERT INTO favorite_relation (video_favorite_BV, user_favorite_Mid) VALUES (?, ?)";
        // SQL 查询：在 favorite_relation 表中删除记录
        String sqlDeleteCollectRelation = "DELETE FROM favorite_relation WHERE video_favorite_BV = ? AND user_favorite_Mid = ?";

        Connection conn = null;
        PreparedStatement pstmtCheckCollect = null;
        PreparedStatement pstmtUpdateCollectRelation = null;

        try {
            // 获取连接并开启事务
            conn = dataSource.getConnection();
//            conn.setAutoCommit(false); // 关闭自动提交

            // 首先检查用户是否已经收藏过视频
            pstmtCheckCollect = conn.prepareStatement(sqlCheckCollect);
            pstmtCheckCollect.setString(1, bv);
            pstmtCheckCollect.setLong(2, mid);
            ResultSet rsCheckCollect = pstmtCheckCollect.executeQuery();

            boolean hasCollected = false;
            if (rsCheckCollect.next()) {
                hasCollected = rsCheckCollect.getInt(1) > 0;
            }

            // 根据收藏状态执行相应的操作
            if (hasCollected) {
                // 用户已收藏，执行取消收藏操作
                pstmtUpdateCollectRelation = conn.prepareStatement(sqlDeleteCollectRelation);
            } else {
                // 用户未收藏，执行收藏操作
                pstmtUpdateCollectRelation = conn.prepareStatement(sqlInsertCollectRelation);
            }

            pstmtUpdateCollectRelation.setString(1, bv);
            pstmtUpdateCollectRelation.setLong(2, mid);
            pstmtUpdateCollectRelation.executeUpdate();

            // 提交事务
//            conn.commit();
            return !hasCollected;
        } catch (SQLException e) {
            e.printStackTrace();
//            if (conn != null) {
//                try {
//                    conn.rollback(); // 发生异常时回滚事务
//                } catch (SQLException ex) {
//                    ex.printStackTrace();
//                }
//            }
            return false;
        }
//        finally {
//            // 关闭资源
//            try {
//                if (pstmtCheckCollect != null) pstmtCheckCollect.close();
//                if (pstmtUpdateCollectRelation != null) pstmtUpdateCollectRelation.close();
//                if (conn != null) conn.close();
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
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