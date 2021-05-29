select Date as "open", CloseDate as "close", Symbol as symbol, CASE IsBuy 
           WHEN 0 
               THEN 'sell'
           ELSE 
				'buy'
       END "action", round(PriceActual,1) as "open price", o.StopLoss as sl, o.TakeProfit as tp, c.Name, ClosePrice as "close price"
FROM 'Order' o join Channel c on (c.Id = o.IdChannel)
where CloseDate is not null and ClosePrice is not null and sl is not null and tp is not null and o.ErrorState is null
--and IdChannel = 1346656223
