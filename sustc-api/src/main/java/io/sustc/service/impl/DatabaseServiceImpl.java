package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;


//zrj-wang
/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
         return Arrays.asList(12210401, 12210403);
    }



    /**
     * Imports data to an empty database.
     * Invalid data will not be provided.
     *
     * @param danmuRecords danmu records parsed from csv
     * @param userRecords  user records parsed from csv
     * @param videoRecords video records parsed from csv
     */
    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {

        //users

        String userSql = "INSERT INTO users (mid, name, sex, birthday, level, coin, sign, identity, password, qq, wechat) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String followingSql = "INSERT INTO following_relation (user_Mid, follow_Mid) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement userPstmt = conn.prepareStatement(userSql);
             PreparedStatement followingPstmt = conn.prepareStatement(followingSql)) {

            for (UserRecord user : userRecords) {
                // 插入用户记录
                userPstmt.setLong(1, user.getMid());
                userPstmt.setString(2, user.getName());
                userPstmt.setString(3, user.getSex());
                userPstmt.setString(4, user.getBirthday());
                userPstmt.setShort(5, user.getLevel());
                userPstmt.setInt(6, user.getCoin());
                userPstmt.setString(7, user.getSign());
                userPstmt.setString(8, user.getIdentity().name());
                userPstmt.setString(9, user.getPassword());
                userPstmt.setString(10, user.getQq());
                userPstmt.setString(11, user.getWechat());
                userPstmt.executeUpdate();

                // 插入关注用户关系
                for (long followMid : user.getFollowing()) {
                    followingPstmt.setLong(1, user.getMid());
                    followingPstmt.setLong(2, followMid);
                    followingPstmt.executeUpdate();
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

            //videos
        String videoSql = "INSERT INTO videos (BV, title, owner_Mid, commit_time, review_time, public_time, duration, description, reviewer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String likedSql = "INSERT INTO liked_relation (video_like_BV, user_like_Mid) VALUES (?, ?)";
        String coinSql = "INSERT INTO coin_relation (video_coin_BV, user_coin_Mid) VALUES (?, ?)";
        String favoriteSql = "INSERT INTO favorite_relation (video_favorite_BV, user_favorite_Mid) VALUES (?, ?)";
        String viewSql = "INSERT INTO view_relation (video_view_BV, user_view_Mid, view_time) VALUES (?, ?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement videoPstmt = conn.prepareStatement(videoSql);
             PreparedStatement likedPstmt = conn.prepareStatement(likedSql);
             PreparedStatement coinPstmt = conn.prepareStatement(coinSql);
             PreparedStatement favoritePstmt = conn.prepareStatement(favoriteSql);
             PreparedStatement viewPstmt = conn.prepareStatement(viewSql)) {

            for (VideoRecord video : videoRecords) {
                // 插入视频记录
                videoPstmt.setString(1, video.getBv());
                videoPstmt.setString(2, video.getTitle());
                videoPstmt.setLong(3, video.getOwnerMid());
                videoPstmt.setTimestamp(4, video.getCommitTime());
                videoPstmt.setTimestamp(5, video.getReviewTime());
                videoPstmt.setTimestamp(6, video.getPublicTime());
                videoPstmt.setFloat(7, video.getDuration());
                videoPstmt.setString(8, video.getDescription());
                if (video.getReviewer() != null) {
                    videoPstmt.setLong(9, video.getReviewer());
                } else {
                    videoPstmt.setNull(9, java.sql.Types.BIGINT);
                }
                videoPstmt.executeUpdate();

                // 插入喜欢视频的用户关系
                for (long likeMid : video.getLike()) {
                    likedPstmt.setString(1, video.getBv());
                    likedPstmt.setLong(2, likeMid);
                    likedPstmt.executeUpdate();
                }

                // 插入投币用户的关系
                for (long coinMid : video.getCoin()) {
                    coinPstmt.setString(1, video.getBv());
                    coinPstmt.setLong(2, coinMid);
                    coinPstmt.executeUpdate();
                }

                // 插入收藏视频的用户关系
                for (long favoriteMid : video.getFavorite()) {
                    favoritePstmt.setString(1, video.getBv());
                    favoritePstmt.setLong(2, favoriteMid);
                    favoritePstmt.executeUpdate();
                }

                // 插入观看视频的用户关系
                if (video.getViewerMids() != null && video.getViewTime() != null) {
                    for (int i = 0; i < video.getViewerMids().length; i++) {
                        viewPstmt.setString(1, video.getBv());
                        viewPstmt.setLong(2, video.getViewerMids()[i]);
                        viewPstmt.setFloat(3, video.getViewTime()[i]);
                        viewPstmt.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        //danmu
        String danmuSql = "INSERT INTO danmu (danmu_BV, danmu_Mid, time, content, postTime) VALUES (?, ?, ?, ?, ?)";
        String likbySql = "INSERT INTO danmuLiked_relation (danmu_liked_id, user_liked_Mid) VALUES (?, ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement danmuPstmt = conn.prepareStatement(danmuSql);
             PreparedStatement likedPstmt = conn.prepareStatement(likbySql)) {
            for (DanmuRecord record : danmuRecords) {
                // 插入danmu记录
                danmuPstmt.setString(1, record.getBv());
                danmuPstmt.setLong(2, record.getMid());
                danmuPstmt.setFloat(3, record.getTime());
                danmuPstmt.setString(4, record.getContent());
                danmuPstmt.setTimestamp(5, record.getPostTime());
                danmuPstmt.executeUpdate();

                // 获取自动生成的danmu ID
                ResultSet generatedKeys = danmuPstmt.getGeneratedKeys();
                if (generatedKeys.next()) {
                    int danmuId = generatedKeys.getInt(1);

                    // 对于每个likedBy用户，插入danmuLiked_relation记录
                    for (long userMid : record.getLikedBy()) {
                        likedPstmt.setInt(1, danmuId);
                        likedPstmt.setLong(2, userMid);
                        likedPstmt.executeUpdate();
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }








    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void truncate() {
        // You can use the default truncate script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
