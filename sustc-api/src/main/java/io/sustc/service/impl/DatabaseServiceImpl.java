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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.mindrot.jbcrypt.BCrypt;

//zrj-wang

//finish,need to test
/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {
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
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {
        try (Connection conn = dataSource.getConnection()) {
            ExecutorService executorService = Executors.newFixedThreadPool(3);

            executorService.submit(() -> importUsers(conn, userRecords));
            executorService.submit(() -> importVideos(conn, videoRecords));
            executorService.submit(() -> importDanmu(conn, danmuRecords));

            executorService.shutdown();

            try {
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            conn.commit(); // 在这里提交事务

            log.info("Import completed.");
        } catch (SQLException e) {
            throw new RuntimeException("Error creating database connection", e);
        }
        System.out.println(danmuRecords.size());
      System.out.println(userRecords.size());
       System.out.println(videoRecords.size());
    }

    private void importUsers(Connection conn, List<UserRecord> userRecords) {
        try {
            // SQL语句
            String userSql = "INSERT INTO users (mid, name, sex, birthday, level, coin, sign, identity, password, qq, wechat) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String followingSql = "INSERT INTO following_relation (user_Mid, follow_Mid) VALUES (?, ?)";

            PreparedStatement userPstmt = conn.prepareStatement(userSql);
            PreparedStatement followingPstmt = conn.prepareStatement(followingSql);

            int count = 0;
            for (UserRecord user : userRecords) {
                // 使用bcrypt加密用户密码
                String encryptedPassword = BCrypt.hashpw(user.getPassword(), BCrypt.gensalt());

                // 设置预处理语句参数
                userPstmt.setLong(1, user.getMid());
                userPstmt.setString(2, user.getName());
                userPstmt.setString(3, user.getSex());
                userPstmt.setString(4, user.getBirthday());
                userPstmt.setShort(5, user.getLevel());
                userPstmt.setInt(6, user.getCoin());
                userPstmt.setString(7, user.getSign());
                userPstmt.setString(8, user.getIdentity().name());
                userPstmt.setString(9, encryptedPassword); // 设置加密后的密码
                userPstmt.setString(10, user.getQq());
                userPstmt.setString(11, user.getWechat());

                userPstmt.addBatch(); // 添加到批处理

                if (++count % 500 == 0) {
                    userPstmt.executeBatch(); // 每500条执行一次批处理
                }
            }
            userPstmt.executeBatch(); // 执行剩余的批处理

            for (UserRecord user : userRecords) {
                for (long followMid : user.getFollowing()) {
                    followingPstmt.setLong(1, user.getMid());
                    followingPstmt.setLong(2, followMid);
                    followingPstmt.addBatch();
                }
                if (++count % 500 == 0) {
                    followingPstmt.executeBatch(); // 执行剩余的批处理
                }
            }
            followingPstmt.executeBatch(); // 执行剩余的批处理

        } catch (SQLException e) {
            throw new RuntimeException("Error importing user data", e);
        }
    }

    private void importVideos(Connection conn, List<VideoRecord> videoRecords) {
        try {
            //videos
            String videoSql = "INSERT INTO videos (BV, title, owner_Mid, commit_time, review_time, public_time, duration, description, reviewer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String likedSql = "INSERT INTO liked_relation (video_like_BV, user_like_Mid) VALUES (?, ?)";
            String coinSql = "INSERT INTO coin_relation (video_coin_BV, user_coin_Mid) VALUES (?, ?)";
            String favoriteSql = "INSERT INTO favorite_relation (video_favorite_BV, user_favorite_Mid) VALUES (?, ?)";
            String viewSql = "INSERT INTO watched_relation (video_watched_BV, user_watched_Mid, watched_time) VALUES (?, ?, ?)";


            PreparedStatement videoPstmt = conn.prepareStatement(videoSql);
            PreparedStatement likedPstmt = conn.prepareStatement(likedSql);
            PreparedStatement coinPstmt = conn.prepareStatement(coinSql);
            PreparedStatement favoritePstmt = conn.prepareStatement(favoriteSql);
            PreparedStatement viewPstmt = conn.prepareStatement(viewSql);

            // Import video data here using the shared connection 'conn'
            // ...
            int count2 = 0;
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
                videoPstmt.setLong(9, video.getReviewer());

                videoPstmt.addBatch(); // 添加到批处理


                // 插入喜欢视频的用户关系
                for (long likeMid : video.getLike()) {
                    likedPstmt.setString(1, video.getBv());
                    likedPstmt.setLong(2, likeMid);
                    likedPstmt.addBatch();
                }

                // 插入投币用户的关系
                for (long coinMid : video.getCoin()) {
                    coinPstmt.setString(1, video.getBv());
                    coinPstmt.setLong(2, coinMid);
                    coinPstmt.addBatch();
                }

                // 插入收藏视频的用户关系
                for (long favoriteMid : video.getFavorite()) {
                    favoritePstmt.setString(1, video.getBv());
                    favoritePstmt.setLong(2, favoriteMid);
                    favoritePstmt.addBatch();
                }

                // 插入观看视频的用户关系
//                    if (video.getViewerMids().length>0 && video.getViewTime().length>0) {
                for (int i = 0; i < video.getViewerMids().length; i++) {
                    viewPstmt.setString(1, video.getBv());
                    viewPstmt.setLong(2, video.getViewerMids()[i]);
                    viewPstmt.setFloat(3, video.getViewTime()[i]);
                    viewPstmt.addBatch();
                }
//                    }

                if (++count2 % 500 == 0) {
                    videoPstmt.executeBatch(); // 每500条执行一次批处理
                    likedPstmt.executeBatch();
                    coinPstmt.executeBatch();
                    favoritePstmt.executeBatch();
                    viewPstmt.executeBatch();
                }
            }

            videoPstmt.executeBatch(); // 执行剩余的批处理
            likedPstmt.executeBatch();
            coinPstmt.executeBatch();
            favoritePstmt.executeBatch();
            viewPstmt.executeBatch();

//ok

        } catch (SQLException e) {
            throw new RuntimeException("Error importing video data", e);
        }
    }

    private void importDanmu(Connection conn, List<DanmuRecord> danmuRecords) {
        try {
            //danmu
            String danmuSql = "INSERT INTO danmu (danmu_BV, danmu_Mid, time, content, postTime) VALUES (?, ?, ?, ?, ?)";
            String likbySql = "INSERT INTO danmuliked_relation (danmu_liked_id, user_liked_Mid) VALUES (?, ?)";

            // Import danmu data here using the shared connection 'conn'
            // ...
            PreparedStatement danmuPstmt = conn.prepareStatement(danmuSql, PreparedStatement.RETURN_GENERATED_KEYS);
            PreparedStatement likedPstmt1 = conn.prepareStatement(likbySql);
            int count3 = 0;

            for (DanmuRecord record : danmuRecords) {
                // 插入danmu记录
                danmuPstmt.setString(1, record.getBv());
                danmuPstmt.setLong(2, record.getMid());
                danmuPstmt.setFloat(3, record.getTime());
                danmuPstmt.setString(4, record.getContent());
                danmuPstmt.setTimestamp(5, record.getPostTime());
                danmuPstmt.executeUpdate();
                ResultSet generatedKeys = danmuPstmt.getGeneratedKeys();
                while (generatedKeys.next()) {
                    int danmuId = generatedKeys.getInt(1);
                    for (long userMid : record.getLikedBy()) {
                        likedPstmt1.setInt(1, danmuId);
                        likedPstmt1.setLong(2, userMid);
                        likedPstmt1.executeUpdate();
                    }
                }
            }
            } catch (SQLException e) {
            throw new RuntimeException("Error importing danmu data", e);
        }
    }
}










//
//    @Override
//    public void importData(
//            List<DanmuRecord> danmuRecords,
//            List<UserRecord> userRecords,
//            List<VideoRecord> videoRecords
//    ) {
//
//        Connection conn = null;
//        try {
//            conn= dataSource.getConnection();
//            conn.setAutoCommit(false); // 禁用自动提交，启用事务
//
//            //users
//
//            String userSql = "INSERT INTO users (mid, name, sex, birthday, level, coin, sign, identity, password, qq, wechat) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
//            String followingSql = "INSERT INTO following_relation (user_Mid, follow_Mid) VALUES (?, ?)";
//            //videos
//            String videoSql = "INSERT INTO videos (BV, title, owner_Mid, commit_time, review_time, public_time, duration, description, reviewer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
//            String likedSql = "INSERT INTO liked_relation (video_like_BV, user_like_Mid) VALUES (?, ?)";
//            String coinSql = "INSERT INTO coin_relation (video_coin_BV, user_coin_Mid) VALUES (?, ?)";
//            String favoriteSql = "INSERT INTO favorite_relation (video_favorite_BV, user_favorite_Mid) VALUES (?, ?)";
//            String viewSql = "INSERT INTO watched_relation (video_watched_BV, user_watched_Mid, watched_time) VALUES (?, ?, ?)";
//            //danmu
//            String danmuSql = "INSERT INTO danmu (danmu_BV, danmu_Mid, time, content, postTime) VALUES (?, ?, ?, ?, ?)";
//            String likbySql = "INSERT INTO danmuliked_relation (danmu_liked_id, user_liked_Mid) VALUES (?, ?)";
//
//            try (
//                 PreparedStatement userPstmt = conn.prepareStatement(userSql);
//                 PreparedStatement followingPstmt = conn.prepareStatement(followingSql);
//
//                 PreparedStatement videoPstmt = conn.prepareStatement(videoSql);
//                 PreparedStatement likedPstmt = conn.prepareStatement(likedSql);
//                 PreparedStatement coinPstmt = conn.prepareStatement(coinSql);
//                 PreparedStatement favoritePstmt = conn.prepareStatement(favoriteSql);
//                 PreparedStatement viewPstmt = conn.prepareStatement(viewSql);
//                 PreparedStatement danmuPstmt = conn.prepareStatement(danmuSql, PreparedStatement.RETURN_GENERATED_KEYS);
//                 PreparedStatement likedPstmt1 = conn.prepareStatement(likbySql)) {
//                int count = 0;
//                for (UserRecord user : userRecords) {
//                    // 插入用户记录
//                    userPstmt.setLong(1, user.getMid());
//                    userPstmt.setString(2, user.getName());
//                    userPstmt.setString(3, user.getSex());
//                    userPstmt.setString(4, user.getBirthday());
//                    userPstmt.setShort(5, user.getLevel());
//                    userPstmt.setInt(6, user.getCoin());
//                    userPstmt.setString(7, user.getSign());
//                    userPstmt.setString(8, user.getIdentity().name());
//                    userPstmt.setString(9, user.getPassword());
//                    userPstmt.setString(10, user.getQq());
//                    userPstmt.setString(11, user.getWechat());
//
//                    userPstmt.addBatch(); // 添加到批处理
//
//
//                    if (++count % 500 == 0) {
//                        userPstmt.executeBatch();
//                    }
//                }
//                userPstmt.executeBatch(); // 执行剩余的批处理
//
//                for (UserRecord user : userRecords) {
//                    for (long followMid : user.getFollowing()) {
//                        followingPstmt.setLong(1, user.getMid());
//                        followingPstmt.setLong(2, followMid);
//                        followingPstmt.addBatch();
//                    }
//                    if (++count % 500 == 0) {
//                        followingPstmt.executeBatch();
//                    }
//                }
//
//                followingPstmt.executeBatch(); // 执行剩余的批处理
//
//
//
//                int count2 = 0;
//                for (VideoRecord video : videoRecords) {
//                    // 插入视频记录
//                    videoPstmt.setString(1, video.getBv());
//                    videoPstmt.setString(2, video.getTitle());
//                    videoPstmt.setLong(3, video.getOwnerMid());
//                    videoPstmt.setTimestamp(4, video.getCommitTime());
//                    videoPstmt.setTimestamp(5, video.getReviewTime());
//                    videoPstmt.setTimestamp(6, video.getPublicTime());
//                    videoPstmt.setFloat(7, video.getDuration());
//                    videoPstmt.setString(8, video.getDescription());
//                    videoPstmt.setLong(9, video.getReviewer());
//
//                    videoPstmt.addBatch(); // 添加到批处理
//
//
//                    // 插入喜欢视频的用户关系
//                    for (long likeMid : video.getLike()) {
//                        likedPstmt.setString(1, video.getBv());
//                        likedPstmt.setLong(2, likeMid);
//                        likedPstmt.addBatch();
//                    }
//
//                    // 插入投币用户的关系
//                    for (long coinMid : video.getCoin()) {
//                        coinPstmt.setString(1, video.getBv());
//                        coinPstmt.setLong(2, coinMid);
//                        coinPstmt.addBatch();
//                    }
//
//                    // 插入收藏视频的用户关系
//                    for (long favoriteMid : video.getFavorite()) {
//                        favoritePstmt.setString(1, video.getBv());
//                        favoritePstmt.setLong(2, favoriteMid);
//                        favoritePstmt.addBatch();
//                    }
//
//                    // 插入观看视频的用户关系
////                    if (video.getViewerMids().length>0 && video.getViewTime().length>0) {
//                        for (int i = 0; i < video.getViewerMids().length; i++) {
//                            viewPstmt.setString(1, video.getBv());
//                            viewPstmt.setLong(2, video.getViewerMids()[i]);
//                            viewPstmt.setFloat(3, video.getViewTime()[i]);
//                            viewPstmt.addBatch();
//                        }
////                    }
//
//                    if (++count2 % 500 == 0) {
//                        videoPstmt.executeBatch(); // 每500条执行一次批处理
//                        likedPstmt.executeBatch();
//                        coinPstmt.executeBatch();
//                        favoritePstmt.executeBatch();
//                        viewPstmt.executeBatch();
//                    }
//                }
//
//                videoPstmt.executeBatch(); // 执行剩余的批处理
//                likedPstmt.executeBatch();
//                coinPstmt.executeBatch();
//                favoritePstmt.executeBatch();
//                viewPstmt.executeBatch();
//
////ok
//
//
//
//                int count3 = 0;
//
//                for (DanmuRecord record : danmuRecords) {
//                    // 插入danmu记录
//                    danmuPstmt.setString(1, record.getBv());
//                    danmuPstmt.setLong(2, record.getMid());
//                    danmuPstmt.setFloat(3, record.getTime());
//                    danmuPstmt.setString(4, record.getContent());
//                    danmuPstmt.setTimestamp(5, record.getPostTime());
//                    danmuPstmt.executeUpdate();
//                    ResultSet generatedKeys = danmuPstmt.getGeneratedKeys();
//                    while (generatedKeys.next()) {
//                        int danmuId = generatedKeys.getInt(1);
//                        for (long userMid : record.getLikedBy()) {
//                            likedPstmt1.setInt(1, danmuId);
//                            likedPstmt1.setLong(2, userMid);
//                            likedPstmt1.executeUpdate();
//                        }
//                    }
//                }
//
//
//            }
//            conn.commit(); // 在这里提交事务
//
//
//        }catch (SQLException e) {
//            if (conn != null) {
//                try {
//                    conn.rollback(); // 在错误情况下回滚事务
//                } catch (SQLException ex) {
//                    throw new RuntimeException("Rollback failed!", ex);
//                }
//            }
//            throw new RuntimeException("Database operation failed", e);
//        } finally {
//            if (conn != null) {
//                try {
//                    conn.setAutoCommit(true); // 恢复自动提交
//                    conn.close();
//                } catch (SQLException ex) {
//                    throw new RuntimeException("Error closing connection", ex);
//                }
//            }
//        }
//
//        // TODO: implement your import logic
//        System.out.println(danmuRecords.size());
//        System.out.println(userRecords.size());
//        System.out.println(videoRecords.size());
//    }
//
//
//
//
//
//
//    /*
//     * The following code is just a quick example of using jdbc datasource.
//     * Practically, the code interacts with database is usually written in a DAO layer.
//     *
//     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
//     */
//
//    @Override
//    public void truncate() {
//        // You can use the default truncate script provided by us in most cases,
//        // but if it doesn't work properly, you may need to modify it.
//
//        String sql = "DO $$\n" +
//                "DECLARE\n" +
//                "    tables CURSOR FOR\n" +
//                "        SELECT tablename\n" +
//                "        FROM pg_tables\n" +
//                "        WHERE schemaname = 'public';\n" +
//                "BEGIN\n" +
//                "    FOR t IN tables\n" +
//                "    LOOP\n" +
//                "        EXECUTE 'TRUNCATE TABLE ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
//                "    END LOOP;\n" +
//                "END $$;\n";
//
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(sql)) {
//            stmt.executeUpdate();
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//
//    @Override
//    public Integer sum(int a, int b) {
//        String sql = "SELECT ?+?";
//
//        try (Connection conn = dataSource.getConnection();
//             PreparedStatement stmt = conn.prepareStatement(sql)) {
//            stmt.setInt(1, a);
//            stmt.setInt(2, b);
//            log.info("SQL: {}", stmt);
//
//            ResultSet rs = stmt.executeQuery();
//            rs.next();
//            return rs.getInt(1);
//        } catch (SQLException e) {
//            throw new RuntimeException(e);
//        }
//    }
//}
