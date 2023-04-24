SELECT
    id,
    null AS `description`,
    0 AS stars,
    null AS thumbnail,
    (
        SELECT
            `room_data_trans.main_name` AS `name`,
            meal AS board,
            (
                SELECT
                    amenities_data AS amenities,
                    `room_data_trans.bedding_type` AS bedding,
                    `room_data_trans.bathroom` AS bathroom,
                    serp_filters AS filters
            ) AS info,
            (
                SELECT
                    show_amount AS amount,
                    show_currency_code AS currency,
                    `type` AS `type`,
                    `vat_data.value` AS vat,
                    `cancellation_penalties.free_cancellation_before` AS grace_period,
                    (
                        SELECT
                            start_at AS `from`,
                            end_at AS `to`,
                            amount_show AS amount
                        FROM
                            `cancellation_penalties.policies`
                    ) AS cancellation_penalties
                FROM
                    `payment_options.payment_types`
            ) AS rates
        FROM
            rates
    ) AS options
FROM
    `$.data.hotels`