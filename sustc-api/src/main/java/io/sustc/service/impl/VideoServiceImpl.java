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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
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
            log.error("Error occurred when likeDanmu", e);
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
        String sql = "SELECT COUNT(*) FROM superusers WHERE mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setLong(1, auth.getMid());

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 检查查询结果
            if (rs.next() && rs.getInt(1) > 0) {
                // 如果计数大于0，当前用户是超级用户
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到超级用户或发生异常，返回 false
        return false;
    }

    private boolean performDeleteVideo(String bv) {
        // SQL 删除语句
        String sql = "DELETE FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, bv);

            // 执行删除
            int rowsAffected = pstmt.executeUpdate();

            // 检查删除结果
            if (rowsAffected > 0) {
                // 如果受影响的行数大于0，删除成功
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 false
        return false;
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
                req.getDuration() < 10 ||
                (req.getPublicTime() != null && req.getPublicTime().toLocalDateTime().isBefore(LocalDateTime.now())) ||
                isTitleExist(auth.getMid(), req.getTitle())) {
            return false;
        }

        // 检查当前用户是否为视频的所有者或超级用户
        if (!isOwner(auth, bv) && !isSuperuser(auth)) {
            return false;
        }

        // 执行更新视频信息的操作
        return performUpdateVideoInfo(bv, req);
    }

    private boolean performUpdateVideoInfo(String bv, PostVideoReq req) {
        // SQL 更新语句
        String sql = "UPDATE videos SET title = ?, public_time = ?, duration = ?, description = ? WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, req.getTitle());
            pstmt.setTimestamp(2, req.getPublicTime());
            pstmt.setLong(3, (long) req.getDuration());
            pstmt.setString(4, req.getDescription());
            pstmt.setString(5, bv);

            // 执行更新
            int rowsAffected = pstmt.executeUpdate();

            // 检查更新结果
            if (rowsAffected > 0) {
                // 如果受影响的行数大于0，更新成功
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 false
        return false;
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
        // SQL 查询语句
        String sql = "SELECT BV FROM videos WHERE title LIKE ? LIMIT ? OFFSET ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, "%" + keywords + "%");
            pstmt.setInt(2, pageSize);
            pstmt.setInt(3, (pageNum - 1) * pageSize);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 检查查询结果
            List<String> bvList = new ArrayList<>();
            while (rs.next()) {
                bvList.add(rs.getString("BV"));
            }
            return bvList;
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 null
        return null;
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
        String sql = "SELECT AVG(view_time / duration) FROM video_records WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, bv);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 检查查询结果
            if (rs.next()) {
                // 如果查询结果不为空，返回平均观看率
                return rs.getDouble(1);
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
            return null;
        }

        // 执行计算热点的操作
        return performGetHotspot(bv);
    }

    private Set<Integer> performGetHotspot(String bv) {
        // SQL 查询语句
        String sql = "SELECT time / 10 AS chunk, COUNT(*) AS cnt FROM danmus WHERE BV = ? GROUP BY chunk ORDER BY cnt DESC";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setString(1, bv);

            // 执行查询
            ResultSet rs = pstmt.executeQuery();

            // 检查查询结果
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 null
        return null;
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

        // 执行审核视频的操作
        return performReviewVideo(bv);
    }
    private boolean performReviewVideo(String bv) {
        // SQL 更新语句
        String sql = "UPDATE videos SET review_time = ? WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setString(2, bv);

            // 执行更新
            int rowsAffected = pstmt.executeUpdate();

            // 检查更新结果
            if (rowsAffected > 0) {
                // 如果受影响的行数大于0，更新成功
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 false
        return false;
    }
    public boolean coinVideo(AuthInfo auth, String bv){
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

        // 执行审核视频的操作
        return performCoinVideo(bv);
    }
    private boolean performCoinVideo(String bv) {
        // SQL 更新语句
        String sql = "UPDATE videos SET coin_time = ? WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setString(2, bv);

            // 执行更新
            int rowsAffected = pstmt.executeUpdate();

            // 检查更新结果
            if (rowsAffected > 0) {
                // 如果受影响的行数大于0，更新成功
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 false
        return false;
    }
    public boolean likeVideo(AuthInfo auth, String bv){
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

        // 执行审核视频的操作
        return performLikeVideo(bv);
    }
    private boolean performLikeVideo(String bv) {
        // SQL 更新语句
        String sql = "UPDATE videos SET like_time = ? WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setString(2, bv);

            // 执行更新
            int rowsAffected = pstmt.executeUpdate();

            // 检查更新结果
            if (rowsAffected > 0) {
                // 如果受影响的行数大于0，更新成功
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 false
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

        // 检查当前用户是否为超级用户


        if (!isSuperuser(auth)) {
            return false;
        }

        // 执行审核视频的操作
        return performCollectVideo(bv);
    }
    private boolean performCollectVideo(String bv) {
        // SQL 更新语句
        String sql = "UPDATE videos SET collect_time = ? WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 设置查询参数
            pstmt.setTimestamp(1, Timestamp.valueOf(LocalDateTime.now()));
            pstmt.setString(2, bv);

            // 执行更新
            int rowsAffected = pstmt.executeUpdate();

            // 检查更新结果
            if (rowsAffected > 0) {
                // 如果受影响的行数大于0，更新成功
                return true;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            // 在生产环境中，应该使用日志记录异常信息，而不是打印堆栈跟踪
        }
        // 如果没有找到视频或发生异常，返回 false
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