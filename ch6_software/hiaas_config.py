customer_configs = [
    {"customer_id": "1235", "subscription": "basic", "bucket": "gs://bestco/bird_data", "db": "postgres"},
    {"customer_id": "3423", "subscription": "plus", "bucket": "gs://for_the_birds", "db": "mysql"},
    {"customer_id": "0953", "subscription":" plus",  "bucket": "s3://dtop324/z342_42ab", "db": "postgres"},
]

subscription_options = [
    {"name": "basic", "stages": ["basic_extract"]},
    {"name": "plus", "stages": ["basic_extract", "ml_query"]},
]