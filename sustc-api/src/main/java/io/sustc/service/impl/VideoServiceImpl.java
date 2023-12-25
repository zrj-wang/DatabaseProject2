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

        // 验证 req 是否有效
        if (req == null || req.getTitle() == null || req.getTitle().isEmpty() ||
                req.getDuration() < 10 ||
                (req.getPublicTime() != null && req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now())) ||
                isTitleExist(auth.getMid(), req.getTitle())) {
            return null;
        }

        String bv;
        do {
            bv = generateBV();
        } while (isBVExist(bv)); // 重复生成BV直到找到一个独一无二的值

        // 实现视频发布的逻辑
        if (insertVideoInfo(auth.getMid(), bv, req)) {
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

            // 直接使用 req.getPublicTime()，无需转换
            pstmt.setTimestamp(6, req.getPublicTime()); // public_time

            pstmt.setLong(7, (long) req.getDuration());
            pstmt.setString(8, req.getDescription());

            int rowsAffected = pstmt.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    private boolean isValidAuth(AuthInfo auth) {
        String sql = "SELECT password, qq, wechat FROM Users WHERE mid = ?";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();
            pstmt = conn.prepareStatement(sql);
            pstmt.setLong(1, auth.getMid());
            rs = pstmt.executeQuery();

            if (rs.next()) {
                String storedPassword = rs.getString("password");
                String encryptedInputPassword = hashPasswordWithSHA256(auth.getPassword());
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
        } finally {
            // 关闭连接、语句和结果集
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
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
        if (qq == null || qq.trim().isEmpty()) {
            return false;
        }
        String sql = "SELECT COUNT(*) FROM Users WHERE qq = ?";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, qq);
            rs = pstmt.executeQuery();

            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 关闭连接、语句和结果集
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return false;
    }




    private boolean checkUserWithWechat(String wechat) {
        if (wechat == null || wechat.trim().isEmpty()) {
            return false;
        }
        String sql = "SELECT COUNT(*) FROM Users WHERE wechat = ?";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, wechat);
            rs = pstmt.executeQuery();

            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 关闭连接、语句和结果集
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }

        return false;
    }



    private boolean checkUserWithBoth(String qq, String wechat) {
        if (qq == null || qq.trim().isEmpty() || wechat == null || wechat.trim().isEmpty()) {
            return false;
        }
        String sql = "SELECT COUNT(*) FROM Users WHERE qq = ? AND wechat = ?";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();
            pstmt = conn.prepareStatement(sql);
            pstmt.setString(1, qq);
            pstmt.setString(2, wechat);
            rs = pstmt.executeQuery();

            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 关闭连接、语句和结果集
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (pstmt != null) {
                try {
                    pstmt.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
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
        String uuid = UUID.randomUUID().toString().substring(0, 10); // 获取UUID的前10个字符
        return "BV" + uuid;
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
        Future<Boolean> future = executorService.submit(() -> {
            // 验证 auth 是否有效
            if (!isValidAuth(auth)) {
                return false;
            }

            // 检查 bv 是否无效
            if (bv == null || bv.isEmpty() || !videoExists(bv)) {
                return false;
            }

            // 检查当前用户是否为视频的所有者或超级用户
            if (!isOwner(auth, bv) && !isSuperuser(auth)) {
                return false;
            }

            // 执行删除视频的操作
            return performDeleteVideo(bv);
        });

        try {
            // 等待异步执行完成并获取结果
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            // 异常处理
            log.error("Error occurred when deleting video", e);
            Thread.currentThread().interrupt(); // 重置中断状态
            return false;
        }
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
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 false
        return false;
    }

    private boolean isOwner(AuthInfo auth, String bv) {
        // SQL 查询语句
        String sql = "SELECT COUNT(*) FROM videos WHERE BV = ? AND owner_Mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, bv);
            pstmt.setLong(2, auth.getMid());

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 检查查询结果
            if (rs.next() && rs.getInt(1) > 0) {
                // 如果计数大于0，当前用户是视频的所有者
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 false
        return false;
    }

    private boolean isSuperuser(AuthInfo auth) {
        // SQL 查询语句
        String sql = "SELECT identity FROM users WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setLong(1, auth.getMid());

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 检查查询结果
            if (rs.next() && "SUPERUSER".equals(rs.getString("identity"))) {
                // 如果identity为SUPERUSER，当前用户是超级用户
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到用户或发生异常，返回 false
        return false;
    }


    private boolean performDeleteVideo(String bv) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            // 开启事务
            conn.setAutoCommit(false);

            // 删除与视频相关的数据
            deleteRelatedData(conn, bv, "danmu", "danmu_BV");
            deleteRelatedData(conn, bv, "liked_relation", "video_like_BV");
            deleteRelatedData(conn, bv, "favorite_relation", "video_favorite_BV");
            deleteRelatedData(conn, bv, "coin_relation", "video_coin_BV");

            // 删除视频本身
            String sqlDeleteVideo = "DELETE FROM videos WHERE BV = ?";
            try (PreparedStatement pstmtDeleteVideo = conn.prepareStatement(sqlDeleteVideo)) {
                pstmtDeleteVideo.setString(1, bv);
                int rowsAffected = pstmtDeleteVideo.executeUpdate();
                if (rowsAffected <= 0) {
                    conn.rollback();
                    return false;
                }
            }

            // 提交事务
            conn.commit();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            // 关闭连接
            if (conn != null) {
                try {
                    conn.setAutoCommit(true);
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    private void deleteRelatedData(Connection conn, String bv, String tableName, String column) throws SQLException {
        String sqlDeleteRelated = "DELETE FROM " + tableName + " WHERE " + column + " = ?";
        try (PreparedStatement pstmtDeleteRelated = conn.prepareStatement(sqlDeleteRelated)) {
            pstmtDeleteRelated.setString(1, bv);
            pstmtDeleteRelated.executeUpdate();
        }
    }

    public boolean updateVideoInfo(AuthInfo auth, String bv, PostVideoReq req) {
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty() || !videoExists(bv)) {
            return false;
        }

        // 检查 req 是否无效
        if (req == null || req.getTitle() == null || req.getTitle().isEmpty() ||
                (req.getPublicTime() != null && req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now()))) {
            return false;
        }

        // 检查当前用户是否为视频的所有者
        if (!isOwner(auth, bv)) {
            return false;
        }

        // 执行更新视频信息的操作
        return performUpdateVideoInfo(bv, req);
    }

    private boolean performUpdateVideoInfo(String bv, PostVideoReq req) {
        // 获取当前视频的信息
        VideoRecord currentInfo = getCurrentVideoInfo(bv);
        if (currentInfo == null) {
            return false; // 视频不存在或无法获取其信息
        }

        // 检查视频时长是否被更改
        if (req.getDuration() != currentInfo.getDuration()) {
            return false; // 不能更改视频时长
        }

        // SQL 更新语句
        String sql = "UPDATE videos SET title = ?, public_time = ?, description = ? WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, req.getTitle());
            pstmt.setTimestamp(2, req.getPublicTime());
            pstmt.setString(3, req.getDescription());
            pstmt.setString(4, bv);

            // 执行更新
            int rowsAffected = pstmt.executeUpdate();

            // 检查更新结果
            if (rowsAffected > 0) {
                // 如果受影响的行数大于0，更新成功
                return currentInfo.getReviewTime() != null; // 如果之前已审核，需要重新审核
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 false
        return false;
    }

    private VideoRecord getCurrentVideoInfo(String bv) {
        String sql = "SELECT bv, title, owner_Mid, commit_time, review_time, public_time, duration, description FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, bv);
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                VideoRecord videoRecord = new VideoRecord();
                videoRecord.setBv(rs.getString("bv"));
                videoRecord.setTitle(rs.getString("title"));
                videoRecord.setOwnerMid(rs.getLong("owner_Mid"));
                // videoRecord.setOwnerName(...); // 需要额外的查询来获取用户名称
                videoRecord.setCommitTime(rs.getTimestamp("commit_time"));
                videoRecord.setReviewTime(rs.getTimestamp("review_time"));
                videoRecord.setPublicTime(rs.getTimestamp("public_time"));
                videoRecord.setDuration(rs.getFloat("duration"));
                videoRecord.setDescription(rs.getString("description"));
                // videoRecord.setReviewer(...); // 如果需要，需要额外的查询来获取审核者信息
                // 其他数组字段（like, coin, favorite, viewerMids, viewTime）也需要额外的查询来填充

                return videoRecord;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        return null;
    }




    public List<String> searchVideo(AuthInfo auth, String keywords, int pageSize, int pageNum) {
        Future<List<String>> future = executorService.submit(() -> {
            // 验证 auth 是否有效
            if (!isValidAuth(auth)) {
                return null;
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
            return performSearchVideo(keywords, pageSize, pageNum);

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

    private List<String> performSearchVideo(String keywords, int pageSize, int pageNum) {
        // 分割关键词
        String[] keywordArray = keywords.split("\\s+");

        // 构建基础查询
        String baseQuery = "SELECT v.BV, ";
        StringBuilder relevanceCalculation = new StringBuilder("0");

        // 为每个关键词添加相关性计算
        for (String keyword : keywordArray) {
            relevanceCalculation.append(" + ")
                    .append("LENGTH(v.title) - LENGTH(REPLACE(LOWER(v.title), LOWER('").append(keyword).append("'), ''))")
                    .append("/LENGTH('").append(keyword).append("')")
                    .append(" + ")
                    .append("LENGTH(v.description) - LENGTH(REPLACE(LOWER(v.description), LOWER('").append(keyword).append("'), ''))")
                    .append("/LENGTH('").append(keyword).append("')")
                    .append(" + ")
                    .append("LENGTH(u.name) - LENGTH(REPLACE(LOWER(u.name), LOWER('").append(keyword).append("'), ''))")
                    .append("/LENGTH('").append(keyword).append("')");
        }

        String fromClause = " FROM videos v JOIN users u ON v.owner_Mid = u.mid LEFT JOIN watched_relation w ON v.BV = w.video_watched_BV ";
        String groupByClause = " GROUP BY v.BV, u.name "; // 包括 u.name 在 GROUP BY 子句中
        String orderByClause = " ORDER BY " + relevanceCalculation.toString() + " DESC, COUNT(w.video_watched_BV) DESC ";
        String limitClause = " LIMIT ? OFFSET ?";

        // 组合最终的 SQL 语句
        String sql = baseQuery + relevanceCalculation.toString() + " AS relevance, COUNT(w.video_watched_BV) as view_count " + fromClause + groupByClause + orderByClause + limitClause;

        // 执行查询并处理结果
        List<String> bvList = new ArrayList<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setInt(1, pageSize);
            pstmt.setInt(2, (pageNum - 1) * pageSize);

            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                bvList.add(rs.getString("BV"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息
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
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
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
        // SQL 查询语句
        String sql = "SELECT CAST(time / 10 AS INTEGER) AS chunk, COUNT(*) AS cnt FROM danmu WHERE danmu_BV = ? GROUP BY chunk ORDER BY cnt DESC";
        Set<Integer> hotspots = new HashSet<>();

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, bv);

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
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果发生异常，返回空集合
        return Collections.emptySet();
    }

    public boolean reviewVideo(AuthInfo auth, String bv) {
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty() || !videoExists(bv)) {
            return false;
        }

        // 检查当前用户是否为超级用户
        if (!isSuperuser(auth)) {
            return false;
        }

        // 检查当前用户是否为视频的所有者
        if (isOwner(auth, bv)) {
            return false;
        }

        // 检查视频是否已经被审核
        if (isVideoReviewed(bv)) {
            return false;
        }

        // 执行审核视频的操作
        return performReviewVideo(bv);
    }

    private boolean performReviewVideo(String bv) {
        // SQL 更新语句
        String sql = "UPDATE videos SET review_time = ? WHERE BV = ? AND review_time IS NULL";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setString(2, bv);

            // 执行更新
            int rowsAffected = pstmt.executeUpdate();

            // 检查更新结果
            return rowsAffected > 0;
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息
        }
        return false;
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

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty() || !videoExists(bv)) {
            return false;
        }

        // 检查用户是否已经给这个视频投过币
        if (hasUserDonatedCoin(auth.getMid(), bv)) {
            return false;
        }

        // 执行投币操作
        return performCoinVideo(auth, bv);
    }

    private boolean performCoinVideo(AuthInfo auth, String bv) {
        Connection conn = null;
        PreparedStatement pstmtCheckCoin = null;
        PreparedStatement pstmtInsertCoin = null;
        PreparedStatement pstmtUpdateUserCoin = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();
            // 开启事务
            conn.setAutoCommit(false);

            // 检查用户是否有足够的币
            String sqlCheckUserCoin = "SELECT coin FROM users WHERE mid = ?";
            pstmtCheckCoin = conn.prepareStatement(sqlCheckUserCoin);
            pstmtCheckCoin.setLong(1, auth.getMid());
            rs = pstmtCheckCoin.executeQuery();

            if (!rs.next() || rs.getInt("coin") <= 0) {
                // 如果用户币不足，返回 false
                conn.rollback();
                return false;
            }

            // 给视频投币
            String sqlInsertCoin = "INSERT INTO coin_relation (video_coin_BV, user_coin_Mid) VALUES (?, ?)";
            pstmtInsertCoin = conn.prepareStatement(sqlInsertCoin);
            pstmtInsertCoin.setString(1, bv);
            pstmtInsertCoin.setLong(2, auth.getMid());
            pstmtInsertCoin.executeUpdate();

            // 更新用户的币数量
            String sqlUpdateUserCoin = "UPDATE users SET coin = coin - 1 WHERE mid = ?";
            pstmtUpdateUserCoin = conn.prepareStatement(sqlUpdateUserCoin);
            pstmtUpdateUserCoin.setLong(1, auth.getMid());
            pstmtUpdateUserCoin.executeUpdate();

            // 提交事务
            conn.commit();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            // 关闭资源
            try {
                if (rs != null) rs.close();
                if (pstmtCheckCoin != null) pstmtCheckCoin.close();
                if (pstmtInsertCoin != null) pstmtInsertCoin.close();
                if (pstmtUpdateUserCoin != null) pstmtUpdateUserCoin.close();
                if (conn != null) {
                    conn.setAutoCommit(true);
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    private boolean hasUserDonatedCoin(long mid, String bv) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM coin_relation WHERE video_coin_BV = ? AND user_coin_Mid = ?")) {
            pstmt.setString(1, bv);
            pstmt.setLong(2, mid);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1) > 0;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }


    public boolean likeVideo(AuthInfo auth, String bv) {
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty() || !videoExists(bv)) {
            return false;
        }

        // 执行点赞视频的操作
        return performLikeVideo(auth, bv);
    }

    private boolean performLikeVideo(AuthInfo auth, String bv) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        PreparedStatement pstmtCheckLike = null;
        PreparedStatement pstmtDeleteLike = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();
            // 开启事务
            conn.setAutoCommit(false);

            // 检查用户是否已经喜欢这个视频
            String sqlCheckLike = "SELECT COUNT(*) FROM liked_relation WHERE video_like_BV = ? AND user_like_Mid = ?";
            pstmtCheckLike = conn.prepareStatement(sqlCheckLike);
            pstmtCheckLike.setString(1, bv);
            pstmtCheckLike.setLong(2, auth.getMid());
            rs = pstmtCheckLike.executeQuery();

            if (rs.next() && rs.getInt(1) > 0) {
                // 如果用户已经喜欢这个视频，取消点赞
                String sqlDeleteLike = "DELETE FROM liked_relation WHERE video_like_BV = ? AND user_like_Mid = ?";
                pstmtDeleteLike = conn.prepareStatement(sqlDeleteLike);
                pstmtDeleteLike.setString(1, bv);
                pstmtDeleteLike.setLong(2, auth.getMid());
                pstmtDeleteLike.executeUpdate();

                conn.commit();
                return false; // 返回取消点赞状态
            } else {
                // 如果用户尚未喜欢这个视频，添加点赞
                String sqlInsertLike = "INSERT INTO liked_relation (video_like_BV, user_like_Mid) VALUES (?, ?)";
                pstmt = conn.prepareStatement(sqlInsertLike);
                pstmt.setString(1, bv);
                pstmt.setLong(2, auth.getMid());
                pstmt.executeUpdate();

                conn.commit();
                return true; // 返回点赞状态
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            // 关闭资源
            try {
                if (rs != null) rs.close();
                if (pstmt != null) pstmt.close();
                if (pstmtCheckLike != null) pstmtCheckLike.close();
                if (pstmtDeleteLike != null) pstmtDeleteLike.close();
                if (conn != null) {
                    conn.setAutoCommit(true);
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    public boolean collectVideo(AuthInfo auth, String bv){
        // 验证 auth 是否有效
        if (!isValidAuth(auth)) {
            return false;
        }

        // 检查 bv 是否无效
        if (bv == null || bv.isEmpty() || !videoExists(bv)) {
            return false;
        }

        // 执行收藏视频的操作
        return performCollectVideo(auth, bv);
    }

    private boolean performCollectVideo(AuthInfo auth, String bv) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = dataSource.getConnection();
            // 开启事务
            conn.setAutoCommit(false);

            // 检查用户是否已经收藏了这个视频
            String sqlCheckCollect = "SELECT COUNT(*) FROM favorite_relation WHERE video_favorite_BV = ? AND user_favorite_Mid = ?";
            pstmt = conn.prepareStatement(sqlCheckCollect);
            pstmt.setString(1, bv);
            pstmt.setLong(2, auth.getMid());
            rs = pstmt.executeQuery();

            if (rs.next() && rs.getInt(1) > 0) {
                // 如果用户已经收藏了这个视频，取消收藏
                String sqlDeleteCollect = "DELETE FROM favorite_relation WHERE video_favorite_BV = ? AND user_favorite_Mid = ?";
                pstmt = conn.prepareStatement(sqlDeleteCollect);
                pstmt.setString(1, bv);
                pstmt.setLong(2, auth.getMid());
                pstmt.executeUpdate();

                conn.commit();
                return false; // 收藏已取消
            } else {
                // 如果用户尚未收藏这个视频，添加收藏
                String sqlInsertCollect = "INSERT INTO favorite_relation (video_favorite_BV, user_favorite_Mid) VALUES (?, ?)";
                pstmt = conn.prepareStatement(sqlInsertCollect);
                pstmt.setString(1, bv);
                pstmt.setLong(2, auth.getMid());
                pstmt.executeUpdate();

                conn.commit();
                return true; // 收藏成功
            }
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                if (conn != null) {
                    conn.rollback();
                }
            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        } finally {
            // 关闭资源
            try {
                if (rs != null) rs.close();
                if (pstmt != null) pstmt.close();
                if (conn != null) {
                    conn.setAutoCommit(true);
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
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