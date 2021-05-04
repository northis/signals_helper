select round(avg(diff),2) as avg_diff, count(IdChannel) as amount, IdChannel, Name, AccessLink
from (select CASE IsBuy 
           WHEN 0 
               THEN o.PriceActual - o.ClosePrice
           ELSE 
				o.ClosePrice - o.PriceActual
       END diff, o.IdChannel as IdChannel, c.Name as Name, c.AccessLink as AccessLink
    from 'Order' o join     'Channel' c
on o.IdChannel = c.Id
    where ErrorState is 'NULL' and CloseDate is not 'NULL' and Symbol = 'XAUUSD') group by IdChannel having amount >50 and avg_diff >0 order by avg_diff desc