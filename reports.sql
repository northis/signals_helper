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
IdChannel in (1439690123,1434334125,1428566201,1410897120,1391702808,1373592578,1370026981,1364375133,1352675414,1346656223,1303758235,1295992076,1286547024,1228916395,1224279642,1211146203,1194806227,1125658955,1113272360,1100562991
) 
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
--and IdChannel in (1439690123,1434334125,1428566201,1410897120,1391702808,1373592578,1370026981,1364375133,1352675414,1346656223,1303758235,1295992076,1286547024,1228916395,1224279642,1194806227,1125658955,1113272360,1100562991,1211146203
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
(1439690123,1434334125,1428566201,1410897120,1391702808,1373592578,1370026981,1364375133,1352675414,1346656223,1303758235,1295992076,1286547024,1228916395,1224279642,1194806227,1125658955,1113272360,1100562991,1211146203)

