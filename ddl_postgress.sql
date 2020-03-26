drop table SPREAD;
CREATE TABLE SPREAD(timestampz TIMESTAMP NOT NULL,location TEXT NOT NULL,locationType TEXT NOT NULL,locationParent TEXT NOT NULL,locationKey TEXT NOT NULL,totalconfirmation INT NULL,totaldeath INT NULL,totalrecovered INT NULL, totallocaltransmission INT NULL, totalexternaltransmission INT NULL,motalityrate DECIMAL NULL)
SELECT * from create_hypertable('SPREAD', 'timestampz','location',4);
delete from SPREAD;
select * from SPREAD order by timestampz desc;
select timestampz as UTC1,timestampz at time zone 'UTC'at time zone 'Asia/Kolkata' as Ist,location,totalconfirmation from SPREAD where timestampz > now() AT TIME ZONE 'UTC' - INTERVAL '100 hours' order by timestampz desc;
select last(timestampz,timestampz) at time zone 'UTC'at time zone 'Asia/Kolkata' as IST,last(location,timestampz) as location,last(totaldeath,timestampz) as death ,last(totalconfirmation,timestampz) as confirmed from SPREAD group by "location";
select last(totalconfirmation,timestampz) as confirmed from SPREAD group by "location";

