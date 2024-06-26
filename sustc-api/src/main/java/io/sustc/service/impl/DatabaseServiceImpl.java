package io.sustc.service.impl;

import io.sustc.dto.DanmuRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.VideoRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;




import java.util.concurrent.*;


//zrj-wang

//finish,need to test
/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {




    @Autowired
    private DataSource dataSource;


    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12210401, 12210403);
    }

    @Override
    public void importData(
            List<DanmuRecord> danmuRecords,
            List<UserRecord> userRecords,
            List<VideoRecord> videoRecords
    ) {

        try (Connection conn= dataSource.getConnection()) {
            conn.setAutoCommit(false);


            //users

            String userSql = "INSERT INTO users (mid, name, sex, birthday, level, coin, sign, identity, password, qq, wechat) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String followingSql = "INSERT INTO following_relation (user_Mid, follow_Mid) VALUES (?, ?)";
            //videos
            String videoSql = "INSERT INTO videos (BV, title, owner_Mid, commit_time, review_time, public_time, duration, description, reviewer) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            String likedSql = "INSERT INTO liked_relation (video_like_BV, user_like_Mid) VALUES (?, ?)";
            String coinSql = "INSERT INTO coin_relation (video_coin_BV, user_coin_Mid) VALUES (?, ?)";
            String favoriteSql = "INSERT INTO favorite_relation (video_favorite_BV, user_favorite_Mid) VALUES (?, ?)";
            String viewSql = "INSERT INTO watched_relation (video_watched_BV, user_watched_Mid, watched_time) VALUES (?, ?, ?)";
            //danmu
            String danmuSql = "INSERT INTO danmu (danmu_BV, danmu_Mid, time, content, postTime) VALUES (?, ?, ?, ?, ?)";
            String likbySql = "INSERT INTO danmuliked_relation (danmu_liked_id, user_liked_Mid) VALUES (?, ?)";


            String open1="create index index1 on watched_relation(video_watched_BV)";
            String open2="create index index2 on watched_relation(user_watched_Mid)";
            String open3="create index index3 on favorite_relation(video_favorite_BV)";
            String open4="create index index4 on favorite_relation(user_favorite_Mid)";
            String open5="create index index5 on coin_relation(video_coin_BV)";
            String open6="create index index6 on coin_relation(user_coin_Mid)";
            String open7="create index index7 on liked_relation(video_like_BV)";
            String open8="create index index8 on liked_relation(user_like_Mid)";
            String open9="create index index9 on following_relation(user_Mid)";
            String open10="create index index10 on following_relation(follow_Mid)";
            String open11="create index index11 on danmuLiked_relation(danmu_liked_id)";
            String open12="create index index12 on danmuLiked_relation(user_liked_Mid)";
            String open13="create index index13 on danmu(danmu_BV)";
            String open14="create index index14 on danmu(danmu_Mid)";

            try (
                 PreparedStatement userPstmt = conn.prepareStatement(userSql);
                 PreparedStatement followingPstmt = conn.prepareStatement(followingSql);

                 PreparedStatement videoPstmt = conn.prepareStatement(videoSql);
                 PreparedStatement likedPstmt = conn.prepareStatement(likedSql);
                 PreparedStatement coinPstmt = conn.prepareStatement(coinSql);
                 PreparedStatement favoritePstmt = conn.prepareStatement(favoriteSql);
                 PreparedStatement viewPstmt = conn.prepareStatement(viewSql);
                 PreparedStatement danmuPstmt = conn.prepareStatement(danmuSql, PreparedStatement.RETURN_GENERATED_KEYS);
                 PreparedStatement likedPstmt1 = conn.prepareStatement(likbySql);
                 PreparedStatement open1stmt = conn.prepareStatement(open1);
                 PreparedStatement open2stmt = conn.prepareStatement(open2);
                 PreparedStatement open3stmt = conn.prepareStatement(open3);
                 PreparedStatement open4stmt = conn.prepareStatement(open4);
                 PreparedStatement open5stmt = conn.prepareStatement(open5);
                 PreparedStatement open6stmt = conn.prepareStatement(open6);
                 PreparedStatement open7stmt = conn.prepareStatement(open7);
                    PreparedStatement open8stmt = conn.prepareStatement(open8);
                    PreparedStatement open9stmt = conn.prepareStatement(open9);
                    PreparedStatement open10stmt = conn.prepareStatement(open10);
                    PreparedStatement open11stmt = conn.prepareStatement(open11);
PreparedStatement open12stmt = conn.prepareStatement(open12);
PreparedStatement open13stmt = conn.prepareStatement(open13);
PreparedStatement open14stmt = conn.prepareStatement(open14)


                 ) {
                int count = 0;


                for (UserRecord user : userRecords) {
//                    String hashedPassword = BCrypt.hashpw(user.getPassword(), BCrypt.gensalt());
                    // 插入用户记录
                    String hashedPassword = hashPasswordWithSHA256(user.getPassword());
//                    String hashedPassword = bcryptPassword(user.getPassword());
//                    String hashedPassword = user.getPassword();



                    userPstmt.setLong(1, user.getMid());
                    userPstmt.setString(2, user.getName());
                    userPstmt.setString(3, user.getSex());
                    userPstmt.setString(4, user.getBirthday());
                    userPstmt.setShort(5, user.getLevel());
                    userPstmt.setInt(6, user.getCoin());
                    userPstmt.setString(7, user.getSign());
                    userPstmt.setString(8, user.getIdentity().name());
                    userPstmt.setString(9, hashedPassword); // 使用 SHA-256 哈希后的密码
                    userPstmt.setString(10, user.getQq());
                    userPstmt.setString(11, user.getWechat());

                    userPstmt.addBatch(); // 添加到批处理


                    if (++count % 500 == 0) {
                        userPstmt.executeBatch();
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
                        followingPstmt.executeBatch();
                    }
                }

                followingPstmt.executeBatch(); // 执行剩余的批处理



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

                open1stmt.execute();
                open2stmt.execute();
                open3stmt.execute();
                open4stmt.execute();
                open5stmt.execute();
                open6stmt.execute();
                open7stmt.execute();
                open8stmt.execute();
                open9stmt.execute();
                open10stmt.execute();
                open11stmt.execute();
                open12stmt.execute();
                open13stmt.execute();
                open14stmt.execute();




            }
            conn.commit(); // 在这里提交事务


        }catch (SQLException e) {
            throw new RuntimeException(e);
        }

        // TODO: implement your import logic
        System.out.println(danmuRecords.size());
        System.out.println(userRecords.size());
        System.out.println(videoRecords.size());
    }

//    private String bcryptPassword(String password) {
//        BCryptPasswordEncoder encoder = new BCryptPasswordEncoder(10);
//        return encoder.encode(password);
//    }



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





