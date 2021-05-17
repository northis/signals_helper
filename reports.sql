select round(avg(diff),2) as avg_diff, round(avg(sl),2) as sl, max(time_h) as time_h_max, round(avg(time_h),1) as time_h_avg, count(IdChannel) as amount, IdChannel, Name, AccessLink
from (select CASE IsBuy 
           WHEN 0 
               THEN o.PriceActual - o.ClosePrice
           ELSE 
				o.ClosePrice - o.PriceActual
       END diff,

        o.IdChannel as IdChannel, c.Name as Name, c.AccessLink as AccessLink , abs(o.PriceSignal - o.StopLoss) as sl, 
		(Select Cast ((JulianDay(o.CloseDate) - JulianDay(o.Date )) * 24 As Integer)) as time_h
    from 'Order' o join         'Channel' c
on o.IdChannel = c.Id
    where ErrorState is NULL and CloseDate is not NULL and Symbol = 'XAUUSD' and abs
(o.PriceActual - o.PriceSignal)<20) group by IdChannel having --amount >50 and time_min_avg <10-- and IdChannel = 1423091964
IdChannel in (1125658955, 1363566763, 1364375133,1207142391, 1255738343,1434334125,1370026981,1410897120,1286547024,1464178896,
1172892101,1192217585,1178704438,1350359025,1365426359,1370304351,1251070444,1346942904,1162071221,
1285501269,1485556871,1271992541,1432124065,1329520494,1486896540, 1225782200,1295992076,1373592578,1428566201,1423091964) 
order by avg_diff desc





select round(avg(diff),2) as avg_diff, count(IdChannel) as amount, round(avg(sl),2) as sl, IdChannel, Name, AccessLink, max(Date) as DateLast
from (select CASE IsBuy 
           WHEN 0 
               THEN o.PriceActual - o.ClosePrice
           ELSE 
				o.ClosePrice - o.PriceActual
       END diff,

        o.IdChannel as IdChannel, c.Name as Name, c.AccessLink as AccessLink , abs(o.PriceSignal - o.StopLoss) as sl, o.Date
    from 'Order' o join          'Channel' c
on o.IdChannel = c.Id
    where ErrorState is NULL and CloseDate is not NULL and Symbol = 'XAUUSD' and abs
(o.PriceActual - o.PriceSignal)<20 
--and IdChannel in (1140957456,1364375133,1255738343,1229796427,1410897120,1286547024,1442725495,1251070444,1346942904,1192217585,1172892101,1350359025,1194806227,1464178896,1365426359,1370304351,1178704438,1162071221,1295992076,1373592578,1225782200,1211124546,1329520494,1428566201, 1207142391, 1155677116,1323793968)
) group by IdChannel having amount > 1000 order by avg_diff desc

select IdOrder, CASE IsBuy 
           WHEN 0 
               THEN round(PriceActual - ClosePrice)
           ELSE 
				round(ClosePrice - PriceActual)
       END diff, abs(PriceSignal - StopLoss) as sl, Date
from 'Order'
where IdChannel = 1373592578 and diff is not null

select Date as "open", Symbol as symbol, CASE IsBuy 
           WHEN 0 
               THEN 'sell'
           ELSE 
				'buy'
       END "action", PriceActual as "open price", c.Name
FROM 'Order' o join Channel c on (c.Id = o.IdChannel)
where CloseDate is not null and ClosePrice is not null

update 'Channel' SET HistoryLoaded = 0, HistoryAnalyzed = 0 where Id in 
(1125658955, 1363566763, 1364375133,1207142391, 1255738343,1434334125,1370026981,1410897120,1286547024,1464178896,
1172892101,1192217585,1178704438,1350359025,1365426359,1370304351,1251070444,1346942904,1162071221,
1285501269,1485556871,1271992541,1432124065,1329520494,1486896540, 1225782200,1295992076,1373592578,1428566201,1423091964,1433602692, 1194806227,1447702777,1159947716) 

