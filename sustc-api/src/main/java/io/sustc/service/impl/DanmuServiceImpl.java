package io.sustc.service.impl;
import io.sustc.dto.AuthInfo;
import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DanmuService;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
//zrj-wang


@Service
@Slf4j
public class DanmuServiceImpl implements DanmuService {
    @Autowired
    private DataSource dataSource;


    @Override
    public long sendDanmu(AuthInfo auth, String bv, String content, float time) {
        if (auth == null || !isValidAuth(auth)) {
            return -1;
        }

        if (bv == null || !videoExists(bv)) {
            return -1;
        }

        if (content == null || content.trim().isEmpty()) {
            return -1;
        }

        if (!isVideoPublished(bv)) {
            return -1;
        }

        if (!hasWatchedVideo(auth.getMid(), bv)) {
            return -1;
        }

        return insertDanmu(bv, auth.getMid(), content, time);
    }

    //检查用户认证信息是否有效
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

    private boolean hasWatchedVideo(long mid, String bv) {
        String sql = "SELECT COUNT(*) FROM watched_relation WHERE user_watched_Mid = ? AND video_view_BV = ?";
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
        String sql = "SELECT public_time, review_time FROM videos WHERE BV = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, bv);
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                Timestamp publicTime = rs.getTimestamp("public_time");
                Timestamp reviewTime = rs.getTimestamp("review_time");

                // 检查public_time是否存在且在review_time之后
                return publicTime != null && (reviewTime == null || publicTime.after(reviewTime));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }




    private long insertDanmu(String bv, long mid, String content, float time) {
        String sql = "INSERT INTO danmu (danmu_BV, danmu_Mid, time, content, postTime) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setString(1, bv);
            pstmt.setLong(2, mid);
            pstmt.setFloat(3, time);
            pstmt.setString(4, content);
            int affectedRows = pstmt.executeUpdate();

            if (affectedRows == 0) {
                throw new SQLException("Creating danmu failed, no rows affected.");
            }

            try (ResultSet generatedKeys = pstmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    return generatedKeys.getLong(1);
                } else {
                    throw new SQLException("Creating danmu failed, no ID obtained.");
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }








    @Override
    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter) {
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
    }



    private boolean isValidTimeRange(String bv, float timeStart, float timeEnd) {
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
            return baseSql + " GROUP BY content ORDER BY MIN(postTime), time";
        } else {
            return baseSql + " ORDER BY time";
        }
    }




    @Override
    public boolean likeDanmu(AuthInfo auth, long id) {
        if (!isValidAuth(auth)) {
            return false;
        }

        // 检查弹幕是否存在
        if (!danmuExists(id)) {
            return false;
        }

        String sql = "SELECT COUNT(*) FROM danmuLiked_relation WHERE danmu_liked_id = ? AND user_liked_Mid = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, id);
            pstmt.setLong(2, auth.getMid());
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


}
