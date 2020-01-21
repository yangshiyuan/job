package com.example.repair.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

@RestController
public class JobDateController {

    private static BigDecimal REVENUE_DAY = sqrtByYear(BigDecimal.valueOf(0.1));

    /**
     * 税率：10%
     */
    public static final BigDecimal RATE_RATIO = BigDecimal.valueOf(0.1);

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String SELECT = "SELECT revenue_id, user_id, accumulate_amount, revenue_amount, reset_zero_amount, rate_amount, calculate_amount, balance, balance_before" +
            " FROM share_revenue_log WHERE batch_no = ? AND balance_before > calculate_amount ORDER BY revenue_id LIMIT 1000";

    private static final String SELECT_PAGE = "SELECT revenue_id, user_id, accumulate_amount, revenue_amount, reset_zero_amount, rate_amount, calculate_amount, balance, balance_before" +
            " FROM share_revenue_log WHERE revenue_id > ? AND batch_no = ? AND balance_before > calculate_amount ORDER BY revenue_id LIMIT 1000";

    private static final String SELECT_ONE = "SELECT revenue_id, user_id, accumulate_amount, revenue_amount, reset_zero_amount, rate_amount, calculate_amount, balance, balance_before" +
            " FROM share_revenue_log WHERE batch_no = ? AND user_id = calculate_amount ORDER BY revenue_id LIMIT 1000";

    @RequestMapping("run")
    public void run() {
        int size = 0;
        long startTime = System.currentTimeMillis();
        System.out.println("同步时间开始");
        String revenueId = null;
        // 重写那天的记录
        String batchNo = "2020-01-16_M100000000_M";
        String time = "2020-01-16";
        while (true) {
            List<RevenueDataLog> logs;
            if (revenueId == null) {
                logs = selectFirst(batchNo);
            } else {
                logs = selectPage(batchNo, revenueId);
            }

            if (CollectionUtils.isEmpty(logs)) {
                break;
            }
            size += logs.size();
            revenueId = logs.get(logs.size() - 1).getRevenueId();
            for (RevenueDataLog log : logs) {

                TransLog log1 = jdbcTemplate.queryForObject("select balance, create_time from share_trans_log where user_id = ? and create_time < ? order by create_time limit 1",
                        (resultSet, i) -> {
                            TransLog transLog = new TransLog();
                            transLog.setAmount(resultSet.getLong("balance"));
                            transLog.setDate(new Date(resultSet.getTimestamp("create_time").getTime()));
                            return transLog;
                        }, log.getUserId(), time);
                Long amount = jdbcTemplate.queryForObject("SELECT sum(amount) FROM share_trans_record WHERE user_id = ? AND create_time > ? AND create_time < ?", (resultSet, i) -> resultSet.getLong(0), log.getUserId(), log1.getDate(), time);
                long balance = log1.getAmount();
                if (amount != null) {
                    balance = balance + amount;
                }
                if (log.getCalculateAmount().longValue() < balance) {
                    log.setBalanceBefore(balance);
                }
            }
            System.out.println("下一个revenueId=" + revenueId + ",size=" + size);
        }
        System.out.println("同步时间结束：size=" + size + ", time=" + (System.currentTimeMillis() - startTime));
    }

    private void update(RevenueLog log) {
        Long asset = log.getBalanceBefore();
        RevenuePOJO revenuePOJO = getRevenueByAssert(BigDecimal.valueOf(asset, 2), REVENUE_DAY);
        Long revenue_amount = revenuePOJO.revenueAmount.multiply(new BigDecimal(100)).longValue();
        BigDecimal reset_zero_amount = revenuePOJO.resetAmount;
        BigDecimal rate_amount = revenuePOJO.rateAmount;
        Long calculate_amount = asset;
        String revenue_id = log.getRevenueId();
        jdbcTemplate.update("UPDATE share_revenue_log SET revenue_amount = ?, reset_zero_amount = ?, rate_amount = ? ,calculate_amount = ? WHERE revenue_id = ?",
                revenue_amount, reset_zero_amount, rate_amount, calculate_amount, revenue_id);
    }

    private List<RevenueDataLog> selectFirst(String batchNo) {
        return jdbcTemplate.query(SELECT, (resultSet, i) -> getRevenueDataLog(resultSet), batchNo);
    }

    private List<RevenueDataLog> selectPage(String batchNo, String revenueId) {
        return jdbcTemplate.query(SELECT_PAGE, (resultSet, i) -> getRevenueDataLog(resultSet), batchNo, revenueId);
    }

    private RevenueDataLog getRevenueDataLog(ResultSet resultSet) throws SQLException {
        RevenueDataLog dataLog = new RevenueDataLog();
        dataLog.setRevenueId(resultSet.getString("revenue_id"));
        dataLog.setUserId(resultSet.getString("user_id"));
        dataLog.setAccumulateAmount(resultSet.getLong("accumulate_amount"));
        dataLog.setRevenueAmount(resultSet.getLong("revenue_amount"));
        dataLog.setResetZeroAmount(resultSet.getBigDecimal("reset_zero_amount"));
        dataLog.setRateAmount(resultSet.getBigDecimal("rate_amount"));
        dataLog.setCalculateAmount(resultSet.getLong("calculate_amount"));
        dataLog.setBalance(resultSet.getLong("balance"));
        dataLog.setBalanceBefore(resultSet.getLong("balance_before"));
        return dataLog;
    }


    private RevenuePOJO getRevenueByAssert(BigDecimal asset, BigDecimal ratioByDay) {
        RevenuePOJO pojo = new RevenuePOJO();
        // 1. 当日收益
        BigDecimal revenue = asset.multiply(ratioByDay);
        pojo.revenue = revenue;

        // 1. 含税价格：归零 -> 抹去小数点2位后的数字
        BigDecimal revenueWithRate = revenue.setScale(2, RoundingMode.DOWN);
        pojo.revenueWithRate = revenueWithRate;

        // 3. 归零金额
        pojo.resetAmount = revenue.subtract(revenueWithRate);

        // 4.计算税率 ：1)计算税额 2)进位 3)获得实际收益
        // 4.1. 税额
        BigDecimal rate = revenueWithRate.multiply(RATE_RATIO);
        pojo.rateAmount = rate.setScale(2, RoundingMode.UP);
        // 4.2. 入账收益
        pojo.revenueAmount = revenueWithRate.subtract(pojo.rateAmount);

        return pojo;
    }

    public static BigDecimal sqrtByYear(BigDecimal m) {
        m = m.add(BigDecimal.ONE);

        BigDecimal sqrtNum = BigDecimal.valueOf(sqrtN(m.doubleValue(), 365));
        // 保留 8 位小数
        sqrtNum = sqrtNum.setScale(8, RoundingMode.DOWN);

        return sqrtNum.subtract(BigDecimal.ONE);
    }

    public static double sqrtN(double m, int n) {
        double sqrtResult = StrictMath.pow(m, (double) 1 / n);
        return sqrtResult;
    }

}

class TransLog {

    private Long amount;

    private Date date;

    public Long getAmount() {
        return amount;
    }

    public void setAmount(Long amount) {
        this.amount = amount;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}

class RevenueDataLog {

    private String revenueId;

    private String userId;

    private Long accumulateAmount;

    private Long revenueAmount;

    private BigDecimal resetZeroAmount;

    private BigDecimal rateAmount;

    private Long calculateAmount;

    private Long balance;

    private Long balanceBefore;

    public String getRevenueId() {
        return revenueId;
    }

    public void setRevenueId(String revenueId) {
        this.revenueId = revenueId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Long getAccumulateAmount() {
        return accumulateAmount;
    }

    public void setAccumulateAmount(Long accumulateAmount) {
        this.accumulateAmount = accumulateAmount;
    }

    public Long getRevenueAmount() {
        return revenueAmount;
    }

    public void setRevenueAmount(Long revenueAmount) {
        this.revenueAmount = revenueAmount;
    }

    public BigDecimal getResetZeroAmount() {
        return resetZeroAmount;
    }

    public void setResetZeroAmount(BigDecimal resetZeroAmount) {
        this.resetZeroAmount = resetZeroAmount;
    }

    public BigDecimal getRateAmount() {
        return rateAmount;
    }

    public void setRateAmount(BigDecimal rateAmount) {
        this.rateAmount = rateAmount;
    }

    public Long getCalculateAmount() {
        return calculateAmount;
    }

    public void setCalculateAmount(Long calculateAmount) {
        this.calculateAmount = calculateAmount;
    }

    public Long getBalance() {
        return balance;
    }

    public void setBalance(Long balance) {
        this.balance = balance;
    }

    public Long getBalanceBefore() {
        return balanceBefore;
    }

    public void setBalanceBefore(Long balanceBefore) {
        this.balanceBefore = balanceBefore;
    }
}

