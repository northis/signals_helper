select round(avg(diff),2) as avg_diff, count(IdChannel) as amount, IdChannel, Name, AccessLink, round(avg(sl),2) as sl
from (select CASE IsBuy 
           WHEN 0 
               THEN o.PriceActual - o.ClosePrice
           ELSE 
				o.ClosePrice - o.PriceActual
       END diff,

        o.IdChannel as IdChannel, c.Name as Name, c.AccessLink as AccessLink , abs(o.PriceSignal - o.StopLoss) as sl
    from 'Order' o join       'Channel' c
on o.IdChannel = c.Id
    where ErrorState is NULL and CloseDate is not NULL and Symbol = 'XAUUSD' and abs
(o.PriceActual - o.PriceSignal)<20) group by IdChannel having amount >50 order by avg_diff desc