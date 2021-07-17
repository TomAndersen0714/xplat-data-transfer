-- Create database
CREATE DATABASE IF NOT EXISTS tmp ON CLUSTER cluster_3s_2r ENGINE = Ordinary;
CREATE DATABASE IF NOT EXISTS xqc_ods ON CLUSTER cluster_3s_2r ENGINE = Ordinary;

-- Create local table
CREATE TABLE tmp.xdqc_kefu_stat_daily_local ON CLUSTER cluster_3s_2r
(
    `_id`                              String,
    `platform`                         String,
    `shop_id`                          String,
    `mp_shop_id`                       String,
    `group`                            String,
    `date`                             Int32,
    `snick`                            String,
    `c_positive_emotion_trigger_count` Int32,
    `c_negative_emotion_trigger_count` Int32,
    `non_custom_over`                  Int32,
    `leak_follow`                      Int32,
    `shortcut_repeat`                  Int32,
    `blunt_refusal`                    Int32,
    `lack_comfort`                     Int32,
    `random_reply`                     Int32,
    `word_reply`                       Int32,
    `sentence_slow`                    Int32,
    `product_unfamiliar`               Int32,
    `activity_unfamiliar`              Int32,
    `serious_timeout`                  Int32,
    `withdraw_msg`                     Int32,
    `single_emoji_reply`               Int32,
    `abnormal_withdraw_msg`            Int32,
    `no_answer_before_transfer`        Int32,
    `un_reply_timeout`                 Int32,
    `demand_digest`                    Int32,
    `product_detail_explain`           Int32,
    `selling_point_convey`             Int32,
    `product_recommend`                Int32,
    `change_reason_of_return`          Int32,
    `actively_follow`                  Int32,
    `out_stock_restore`                Int32,
    `actively_communicate`             Int32,
    `shop_guarantee`                   Int32,
    `remind_payment`                   Int32,
    `check_address`                    Int32,
    `lead_to_high_evaluate`            Int32,
    `good_ending`                      Int32,
    `c_word_count`                     Int32,
    `kf_word_count`                    Int32,
    `score`                            Int32,
    `score_add`                        Int32,
    `update_time`                      String,
    `create_time`                      String,
    `dialog_count`                     Int32,
    `human_tag_count_stats`            String,
    `emotion_tag_count_stats`          String,
    `abnormal_dialog_count`            Int32,
    `abnormal_dialog_stats`            String,
    `emotion_dialog_stats`             String,
    `c_negative_emotion_dialog_count`  Int32,
    `rule_count_stats`                 String
) ENGINE = ReplicatedMergeTree(
           '/clickhouse/tmp/tables/{layer}_{shard}/xdqc_kefu_stat_daily_local',
           '{replica}'
    )
      PARTITION BY (`date`)
      ORDER BY (`date`, `shop_id`, `snick`)
      SETTINGS index_granularity = 8192,storage_policy = 'rr';

-- Create distributed table
CREATE TABLE tmp.xdqc_kefu_stat_daily_all ON CLUSTER cluster_3s_2r
    AS tmp.xdqc_kefu_stat_daily_local
        ENGINE = Distributed('cluster_3s_2r', 'tmp', 'xdqc_kefu_stat_daily_local', rand());

-- Create local table
CREATE TABLE xqc_ods.xdqc_kefu_stat_local ON CLUSTER cluster_3s_2r
    AS tmp.xdqc_kefu_stat_daily_local
        ENGINE = ReplicatedMergeTree(
                 '/clickhouse/xqc_ods/tables/{layer}_{shard}/xdqc_kefu_stat_local',
                 '{replica}'
            )
            PARTITION BY (`date`)
            ORDER BY (`date`, `shop_id`, `snick`)
            SETTINGS index_granularity = 8192, storage_policy = 'rr';

-- Create distributed table
CREATE TABLE xqc_ods.xdqc_kefu_stat_all ON CLUSTER cluster_3s_2r
    AS xqc_ods.xdqc_kefu_stat_local
        ENGINE = Distributed('cluster_3s_2r', 'xqc_ods', 'xdqc_kefu_stat_local', rand());
