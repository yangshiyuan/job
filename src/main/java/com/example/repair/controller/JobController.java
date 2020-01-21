package com.example.repair.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;

@RestController
public class JobController {

    private static BigDecimal REVENUE_DAY = sqrtByYear(BigDecimal.valueOf(0.1));

    /**
     * 税率：10%
     */
    public static final BigDecimal RATE_RATIO = BigDecimal.valueOf(0.1);

    @Autowired
    JdbcTemplate jdbcTemplate;

    private static final String SELECT = "SELECT revenue_id, balance_before FROM share_revenue_log WHERE batch_no = '2020-01-16_M100000000_M' AND balance_before > calculate_amount ORDER BY revenue_id LIMIT 1000";

    private static final String SELECT_PAGE = "SELECT revenue_id, balance_before FROM share_revenue_log WHERE revenue_id > ? AND batch_no = '2020-01-16_M100000000_M' AND balance_before > calculate_amount ORDER BY revenue_id LIMIT 1000";

    @RequestMapping("run/one")
    public void runOne() {
        RevenueLog log = jdbcTemplate.query("SELECT revenue_id, balance_before FROM share_revenue_log WHERE batch_no = '2020-01-16_M100000000_M' AND balance_before > calculate_amount ORDER BY revenue_id LIMIT 1", (resultSet, i) -> {
            String revenue_id = resultSet.getString("revenue_id");
            Long balance_before = Long.parseLong(resultSet.getString("balance_before"));
            return new RevenueLog(revenue_id, balance_before);
        }).get(0);
        update(log);
    }

    @RequestMapping("run")
    public void run() {
        int size = 0;
        long startTime = System.currentTimeMillis();
        System.out.println("同步时间开始");
        String revenueId = null;
        while (true) {
            List<RevenueLog> logs;
            if (revenueId == null) {
                logs = selectFirst();
            } else {
                logs = selectPage(revenueId);
            }

            if (CollectionUtils.isEmpty(logs)) {
                break;
            }
            size += logs.size();
            revenueId = logs.get(logs.size() - 1).getRevenueId();
            for (RevenueLog log : logs) {
                update(log);
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

    private List<RevenueLog> selectPage(String revenueId) {
        return jdbcTemplate.query(SELECT_PAGE, (resultSet, i) -> {
            String revenue_id = resultSet.getString("revenue_id");
            Long balance_before = Long.parseLong(resultSet.getString("balance_before"));
            return new RevenueLog(revenue_id, balance_before);
        }, revenueId);
    }

    private List<RevenueLog> selectFirst() {
        return jdbcTemplate.query(SELECT, (resultSet, i) -> {
            String revenue_id = resultSet.getString("revenue_id");
            Long balance_before = Long.parseLong(resultSet.getString("balance_before"));
            return new RevenueLog(revenue_id, balance_before);
        });
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

class RevenuePOJO {
    /**
     * 当日收益
     */
    BigDecimal revenue;
    /**
     * 日收益：含税
     */
    BigDecimal revenueWithRate;
    /**
     * 归零金额
     */
    BigDecimal resetAmount;
    /**
     * 税额
     */
    BigDecimal rateAmount;
    /**
     * 入账收益
     */
    BigDecimal revenueAmount;
}


class RevenueLog {

    public RevenueLog() {
    }

    public RevenueLog(String revenueId, Long balanceBefore) {
        this.revenueId = revenueId;
        this.balanceBefore = balanceBefore;
    }

    private String revenueId;

    private Long balanceBefore;

    public String getRevenueId() {
        return revenueId;
    }

    public void setRevenueId(String revenueId) {
        this.revenueId = revenueId;
    }

    public Long getBalanceBefore() {
        return balanceBefore;
    }

    public void setBalanceBefore(Long balanceBefore) {
        this.balanceBefore = balanceBefore;
    }
}
