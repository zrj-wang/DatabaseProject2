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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
//zrj-wang


//@Service
//@Slf4j
//public class DanmuServiceImpl implements DanmuService {
//    @Autowired
//    private DataSource dataSource;
//
//    @Override
//   public long sendDanmu(AuthInfo auth, String bv, String content, float time){
//
//   }
//
//    @Override
//    public List<Long> displayDanmu(String bv, float timeStart, float timeEnd, boolean filter){
//
//   }
//
//    @Override
//    public boolean likeDanmu(AuthInfo auth, long danmuId){
//
//   }
//
//
//
//}
