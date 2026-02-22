SELECT 
    review_id,
    order_id,
    review_score::integer as review_score,
    review_comment_title::TEXT as review_comment_title,
    review_comment_message::TEXT as review_comment_message,
    review_creation_date::timestamp_ntz as review_creation_date,
    review_answer_timestamp::timestamp_ntz as review_answer_date
FROM {{ source('raw', 'raw_reviews')}}