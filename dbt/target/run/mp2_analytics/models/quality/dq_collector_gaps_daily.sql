
  
    
    
    
        
         


        
  

  insert into `mp2`.`dq_collector_gaps_daily`
        ("source", "stream", "day_utc", "gaps", "gap_sec_total", "gap_sec_max", "reasons")
SELECT source, stream, toDate(from_ts) AS day_utc, count() AS gaps,
       round(sum(toUnixTimestamp64Milli(to_ts) - toUnixTimestamp64Milli(from_ts)) / 1000, 1) AS gap_sec_total,
       round(max(toUnixTimestamp64Milli(to_ts) - toUnixTimestamp64Milli(from_ts)) / 1000, 1) AS gap_sec_max,
       groupArray(reason) AS reasons
FROM `mp2`.`collector_gaps` GROUP BY source, stream, day_utc
  