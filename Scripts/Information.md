Bronze Bucket Name --> "youtube-data-gr-bronze"
Silver Bucket Name --> "youtube-data-gr-silver"
Gold Bucket Name --> "youtube-data-gr-gold"

Script Bucket Name --> "youtube-data-gr-script"

SNS ARN --> arn:aws:sns:eu-north-1:225619512731:yt-data-pipeline-alerts-dev


Glue Databases
Glue Bronze - "youtube-data-gr-bronze"
Glue Silver - "youtube-data-gr-silver"
Glue Gold - "youtube-data-gr-gold"

# Glue Bronze to Silver ETL Job Paramaters 
--bronze_database - "youtube-data-gr-bronze"
--bronze table - "raw_statistics"
--silver bucket - "youtube-data-gr-silver"
--silver database - "youtube-data-gr-silver"
--silver table - "clean_statistics"
--silver path - "s3://youtube-data-gr-silver/youtube/"

# Glue Silver to Gold ETL Job Paramaters
--silver_database - "youtube-data-gr-silver"
--gold_bucket - "youtube-data-gr-gold"
--gold_database - "youtube-data-gr-gold"
