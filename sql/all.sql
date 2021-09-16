select round(avg(diff*10),1) as avg_diff,
	round(max(diff*10),1) as avg_max,
	round(min(diff*10),1) as avg_min,
	round(avg(sl*10),1) as sl,
	max(time_h) as time_h_max,
	max(date_open) as last_date,
	round(avg(time_h),1) as time_h_avg,
	count(IdChannel) as amount,
	IdChannel, Name, AccessLink
from (select CASE IsBuy WHEN 0 THEN o.PriceActual - o.ClosePrice  ELSE 				o.ClosePrice - o.PriceActual       END diff,
		o.IdChannel as IdChannel,
		o.Date as date_open,
		c.Name as Name,
		c.AccessLink as AccessLink ,
		abs(o.PriceActual - o.StopLoss) as sl,
		(Select Cast ((JulianDay(o.CloseDate) - JulianDay(o.Date )) * 24 As Integer)) as time_h
	from 'Order' o join          'Channel' c
on o.IdChannel = c.Id
    where ErrorState is NULL and CloseDate is not NULL and Symbol = 'XAUUSD' and abs
(o.PriceActual - o.PriceSignal)<20) group by IdChannel 
having last_date > '2021-06-01' and amount >10
order by avg_diff desc

